# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from math import sqrt

import dz2es
from dz2es.util.collections import MIN, MAX
from dz2es.util.env.profiles import Profiler
from dz2es.util.maths import Math
from dz2es.util.maths.stats import Z_moment, z_moment2stats
from dz2es.util.struct import Struct
from dz2es.util.structs.wraps import wrap
from dz2es.util.times.timer import Timer
from dz2es.util.sql.db import DB
from dz2es.util.env.logs import Log
from dz2es.util.queries import Q


DEBUG = False
ARRAY_TOO_BIG = 1000


class DZ_to_ES():
    def __init__(self, pushlog_settings):
        with Timer("get pushlog"):
            if pushlog_settings.disable:
                all_pushlogs = []
            else:
                with DB(pushlog_settings) as db:
                    all_pushlogs = db.query("""
                        SELECT
                            pl.`date`,
                            left(ch.node, 12) revision,
                            coalesce(bm.alt_name, br.name) branch
                        FROM
                            changesets ch
                        LEFT JOIN
                            pushlogs pl ON pl.id = ch.pushlog_id
                        LEFT JOIN
                            branches br ON br.id = pl.branch_id
                        LEFT JOIN
                            branch_map bm ON br.id = bm.id
                    """)
            Log.note("Got pushlog, now indexing...")
            self.pushlog = Q.index(all_pushlogs, ["branch", "revision"])._data
            self.unknown_branches = set()

    def __del__(self):
        try:
            Log.println("Branches missing from pushlog:\n{{list}}", {"list": self.unknown_branches})
        except Exception, e:
            pass


    # A SIMPLE TRANSFORM OF DATA:  I WOULD ALSO LIKE TO ADD DIMENSIONAL TYPE INFORMATION
    # WHICH WOULD GIVE DEAR READER A BETTER FEEL FOR THE TOTALITY OF THIS DATA
    # BUT THEN AGAIN, SIMPLE IS BETTER, YES?
    def transform(self, id, datazilla):
        try:
            r = datazilla.json_blob

            #ADD DATAZILLA MARKUP
            r.datazilla = {
                "id": id,
                "date_loaded": datazilla.date_loaded * 1000,
                "error_flag": datazilla.error_flag,
                "test_run_id": datazilla.test_run_id,
                "processed_flag": datazilla.processed_flag,
                "error_msg": datazilla.error_msg
            }

            #CONVERT UNIX TIMESTAMP TO MILLISECOND TIMESTAMP
            r.testrun.date *= 1000

            def mainthread_transform(r):
                if r == None:
                    return None

                output = Struct()

                for i in r.mainthread_readbytes:
                    output[i[1].replace(".", "\.")].name = i[1]
                    output[i[1].replace(".", "\.")].readbytes = i[0]
                r.mainthread_readbytes = None

                for i in r.mainthread_writebytes:
                    output[i[1].replace(".", "\.")].name = i[1]
                    output[i[1].replace(".", "\.")].writebytes = i[0]
                r.mainthread_writebytes = None

                for i in r.mainthread_readcount:
                    output[i[1].replace(".", "\.")].name = i[1]
                    output[i[1].replace(".", "\.")].readcount = i[0]
                r.mainthread_readcount = None

                for i in r.mainthread_writecount:
                    output[i[1].replace(".", "\.")].name = i[1]
                    output[i[1].replace(".", "\.")].writecount = i[0]
                r.mainthread_writecount = None

                r.mainthread = output.values()

            mainthread_transform(r.results_aux)
            mainthread_transform(r.results_xperf)

            #ADD PUSH LOG INFO
            try:
                branch = r.test_build.branch
                if branch.endswith("-Non-PGO"):
                    r.test_build.branch = branch
                    r.test_build.pgo = False
                    branch = branch[0:-8]
                else:
                    r.test_build.pgo = True

                with Profiler("get from pushlog"):
                    if self.pushlog.get(branch, None):
                        possible_dates = wrap(self.pushlog[branch].get(r.test_build.revision, None))
                        if not possible_dates:
                            Log.note("{{branch}} @ {{revision}} has no pushlog", r.test_build)
                            r.test_build.push_date = r.datazilla.date_loaded,
                        else:
                            r.test_build.push_date = int(Math.round(possible_dates[0].date * 1000))
                    else:
                        self.unknown_branches.add(branch)
                        Log.note("Whole branch {{branch}} has no pushlog", {"branch":branch})
            except Exception, e:
                Log.warning("{{branch}} @ {{revision}} has no pushlog", r.test_build, e)

            new_records = []

            # RECORD THE UNKNOWN PART OF THE TEST RESULTS
            remainder = r.copy()
            remainder.results = None
            if len(remainder.keys()) > 4:
                new_records.append(remainder)

            #RECORD TEST RESULTS
            if r.testrun.suite in ["dromaeo_css", "dromaeo_dom"]:
                #dromaeo IS SPECIAL, REPLICATES ARE IN SETS OF FIVE
                #RECORD ALL RESULTS
                for i, (test_name, replicates) in enumerate(r.results.items()):
                    for g, sub_results in Q.groupby(replicates, size=5):
                        new_record = Struct(
                            test_machine=r.test_machine,
                            datazilla=r.datazilla,
                            testrun=r.testrun,
                            test_build=r.test_build,
                            result={
                                "test_name": unicode(test_name) + "." + unicode(g),
                                "ordering": i,
                                "samples": sub_results
                            }
                        )
                        try:
                            new_record.result.stats = stats(sub_results)
                        except Exception, e:
                            Log.warning("can not reduce series to moments", e)
                        new_records.append(new_record)
            else:
                for i, (test_name, replicates) in enumerate(r.results.items()):
                    new_record = Struct(
                        test_machine=r.test_machine,
                        datazilla=r.datazilla,
                        testrun=r.testrun,
                        test_build=r.test_build,
                        result={
                            "test_name": test_name,
                            "ordering": i,
                            "samples": replicates
                        }
                    )
                    try:
                        new_record.result.stats = stats(replicates)
                    except Exception, e:
                        Log.warning("can not reduce series to moments", e)
                    new_records.append(new_record)

            return new_records
        except Exception, e:
            Log.error("Transformation failure", e)

def stats(values):
    """
    RETURN LOTS OF AGGREGATES
    """
    if values == None:
        return None

    values = values.map(float, includeNone=False)

    z = Z_moment.new_instance(values)
    s = Struct()
    for k, v in z.dict.items():
        s[k] = v
    for k, v in z_moment2stats(z).items():
        s[k] = v
    s.max = MAX(values)
    s.min = MIN(values)
    s.median = dz2es.util.stats.median(values, simple=False)
    s.last = values.last()
    s.first = values[0]
    if Math.is_number(s.variance) and not Math.is_nan(s.variance):
        s.std = sqrt(s.variance)

    return s
