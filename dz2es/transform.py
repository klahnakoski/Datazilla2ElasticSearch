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
import datetime

from dz2es.pushlog import Pushlog
import pyLibrary
from pyLibrary.collections import MIN, MAX
from pyLibrary.env.profiles import Profiler
from pyLibrary.maths import Math
from pyLibrary.maths.stats import Stats, ZeroMoment2Stats, ZeroMoment
from pyLibrary.structs import Struct, nvl
from pyLibrary.structs import literal_field
from pyLibrary.structs.lists import StructList
from pyLibrary import convert

from pyLibrary.thread.threads import Lock
from pyLibrary.env.logs import Log
from pyLibrary.queries import Q


DEBUG = False
ARRAY_TOO_BIG = 1000
NOW = datetime.datetime.utcnow()
TOO_OLD = NOW - datetime.timedelta(days=30)
PUSHLOG_TOO_OLD = NOW - datetime.timedelta(days=7)


class DZ_to_ES():
    def __init__(self, pushlog_settings):
        if pushlog_settings.disable:
            self.pushlog=None
        else:
            self.pushlog = Pushlog()
        self.locker = Lock()


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
                    output[literal_field(i[1])].name = i[1]
                    output[literal_field(i[1])].readbytes = i[0]
                r.mainthread_readbytes = None

                for i in r.mainthread_writebytes:
                    output[literal_field(i[1])].name = i[1]
                    output[literal_field(i[1])].writebytes = i[0]
                r.mainthread_writebytes = None

                for i in r.mainthread_readcount:
                    output[literal_field(i[1])].name = i[1]
                    output[literal_field(i[1])].readcount = i[0]
                r.mainthread_readcount = None

                for i in r.mainthread_writecount:
                    output[literal_field(i[1])].name = i[1]
                    output[literal_field(i[1])].writecount = i[0]
                r.mainthread_writecount = None

                r.mainthread = output.values()

            mainthread_transform(r.results_aux)
            mainthread_transform(r.results_xperf)


            branch = r.test_build.branch
            if branch.lower().endswith("-non-pgo"):
                branch = branch[0:-8]
                r.test_build.branch = branch
                r.test_build.pgo = False
            else:
                r.test_build.pgo = True

            if r.test_machine.osversion.endswith(".e"):
                r.test_machine.osversion = r.test_machine.osversion[:-2]
                r.test_machine.e10s = True


            #ADD PUSH LOG INFO
            try:
                with Profiler("get from pushlog"):
                    pushdate = None

                    with self.locker:
                        if self.pushlog:
                            pushdate = self.pushlog[branch, r.test_build.revision]

                    if pushdate:
                        r.test_build.push_date = int(Math.round(pushdate * 1000))
                    else:
                        if r.test_build.revision == 'NULL':
                            r.test_build.no_pushlog = True  # OOPS! SOMETHING BROKE
                        elif convert.milli2datetime(Math.min(r.testrun.date, r.datazilla.date_loaded)) < PUSHLOG_TOO_OLD:
                            if self.pushlog:
                                Log.note("{{branch}} @ {{revision}} has no pushlog, transforming anyway", r.test_build)
                            r.test_build.no_pushlog = True
                            r.test_build.push_date = Math.min(r.testrun.date, r.datazilla.date_loaded)  #GIVE IT A DATE
                        else:
                            Log.note("{{branch}} @ {{revision}} has no pushlog, try again later", r.test_build)
                            return []  # TRY AGAIN LATER

            except Exception, e:
                Log.warning("{{branch}} @ {{revision}} has no pushlog", r.test_build, e)

            new_records = []

            # RECORD THE UNKNOWN PART OF THE TEST RESULTS
            remainder = r.copy()
            remainder.results = None
            if not r.results or len(remainder.keys()) > 4:
                new_records.append(remainder)

            #RECORD TEST RESULTS
            total = StructList()
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
                            s = stats(sub_results)
                            new_record.result.stats = s
                            total.append(s)
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
                        s = stats(replicates)
                        new_record.result.stats = s
                        total.append(s)
                    except Exception, e:
                        Log.warning("can not reduce series to moments", e)
                    new_records.append(new_record)

            if len(total) > 1:
                # ADD RECORD FOR GEOMETRIC MEAN SUMMARY
                new_record = Struct(
                    test_machine=r.test_machine,
                    datazilla=r.datazilla,
                    testrun=r.testrun,
                    test_build=r.test_build,
                    result={
                        "test_name": "_"+r.testrun.suite+"_summary",
                        "ordering": -1,
                        "stats": geo_mean(total)
                    }
                )
                new_records.append(new_record)

                # ADD RECORD FOR GRAPH SERVER SUMMARY
                new_record = Struct(
                    test_machine=r.test_machine,
                    datazilla=r.datazilla,
                    testrun=r.testrun,
                    test_build=r.test_build,
                    result={
                        "test_name": "_"+r.testrun.suite+"_old_summary",
                        "ordering": -1,
                        "stats": Stats(samples=Q.sort(total.mean).leftBut(1))
                    }
                )
                new_records.append(new_record)

            return new_records
        except Exception, e:
            Log.error("Transformation failure on id={{id}}", {"id": id}, e)


def stats(values):
    """
    RETURN LOTS OF AGGREGATES
    """
    if values == None:
        return None

    values = values.map(float, includeNone=False)

    z = ZeroMoment.new_instance(values)
    s = Struct()
    for k, v in z.dict.items():
        s[k] = v
    for k, v in ZeroMoment2Stats(z).items():
        s[k] = v
    s.max = MAX(values)
    s.min = MIN(values)
    s.median = pyLibrary.maths.stats.median(values, simple=False)
    s.last = values.last()
    s.first = values[0]
    if Math.is_number(s.variance) and not Math.is_nan(s.variance):
        s.std = sqrt(s.variance)

    return s


def geo_mean(values):
    """
    GIVEN AN ARRAY OF dicts, CALC THE GEO-MEAN ON EACH ATTRIBUTE
    """
    agg = Struct()
    for d in values:
        for k, v in d.items():
            if v != 0:
                agg[k] = nvl(agg[k], ZeroMoment.new_instance()) + Math.log(Math.abs(v))
    return {k: Math.exp(v.stats.mean) for k, v in agg.items()}


