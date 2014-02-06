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
from string import replace
import dz2es
from dz2es.util.math.maths import Math
from dz2es.util.struct import Struct
from dz2es.util.timer import Timer
from dz2es.util.cnv import CNV
from dz2es.util.sql.db import DB
from dz2es.util.env.logs import Log
from dz2es.util.queries import Q
from dz2es.util.stats import Z_moment, z_moment2stats


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
            self.pushlog = Q.index(all_pushlogs, ["branch", "revision"])
            self.unknown_branches = set()

    def __del__(self):
        try:
            Log.println("Branches missing from pushlog:\n{{list}}", {"list": self.unknown_branches})
        except Exception, e:
            pass


    # A SIMPLE TRANSFORM OF DATA:  I WOULD ALSO LIKE TO ADD DIMENSIONAL TYPE INFORMATION
    # WHICH WOULD GIVE DEAR READER A BETTER FEEL FOR THE TOTALITY OF THIS DATA
    # BUT THEN AGAIN, SIMPLE IS BETTER, YES?
    def transform(self, id, datazilla, keep_arrays_smaller_than=ARRAY_TOO_BIG):
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

            new_results = {}
            for i, (k, v) in enumerate(r.results.items()):
                k = replace(k, ".", "_dot_")  # '.' MAKES SEARCH DIFFICULT IN ES

                try:
                    new_results[k] = stats(v)
                except Exception, e:
                    Log.error("can not reduce series to moments", e)

            r.results = new_results

            #CONVERT FROM <name>:<samples> TO {"name":<name>, "samples":<samples>}
            #USING stack() WOULD BE CLEARER, BUT DOES NOT HANDLE THE TOO-LARGE SEQUENCES
            #    r.results=Q.stack([r.results], column="name")
            #    r.results=[{
            #        "name":k,
            #        "moments": Z_moment.new_instance(v).dict,
            #        "samples": (dict(("s"+right("00"+str(i), 2), s) for i, s in enumerate(v)) if len(v)<=keep_arrays_smaller_than else Null)
            #    } for k,v in r.results.items()]

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

            #COLAPSE THESE TO SIMPLE MOMENTS
            if r.results_aux:
                r.results_aux.responsivness = {"moments": CNV.z_moment2dict(Z_moment.new_instance(r.results_aux.responsivness))}
                r.results_aux["Private bytes"] = {"moments": CNV.z_moment2dict(Z_moment.new_instance(r.results_aux["Private bytes"]))}
                r.results_aux.Main_RSS = {"moments": CNV.z_moment2dict(Z_moment.new_instance(r.results_aux.Main_RSS))}
                r.results_aux.shutdown = {"moments": CNV.z_moment2dict(Z_moment.new_instance(r.results_aux.shutdown))}

            mainthread_transform(r.results_aux)
            mainthread_transform(r.results_xperf)

            #    summarize(r.dict, keep_arrays_smaller_than)

            #ADD PUSH LOG INFO
            try:
                branch = r.test_build.branch
                if branch.endswith("-Non-PGO"):
                    branch = branch[0:-8]
                if branch in self.pushlog:
                    possible_dates = self.pushlog[branch][r.test_build.revision]
                    r.test_build.push_date = int(possible_dates[0].date) * 1000
                else:
                    self.unknown_branches.add(branch)
            except Exception, e:
                Log.warning("{{branch}} @ {{revision}} has no pushlog", r.test_build, e)

            return r
        except Exception, e:
            Log.error("Transformation failure", e)


# RESPONSIBLE FOR CONVERTING LONG ARRAYS OF NUMBERS TO THEIR REPRESENTATIVE
# MOMENTS
def summarize(path, r, keep_arrays_smaller_than=25):
    try:
        if isinstance(r, dict):
            for k, v in [(k, v) for k, v in r.items()]:
                new_v = summarize(path + "." + k, v, keep_arrays_smaller_than)
                if isinstance(new_v, Z_moment):
                    #CONVERT MOMENTS' TUPLE TO NAMED HASH (FOR EASIER ES INDEXING)
                    new_v = {"moment": CNV.z_moment2dict(new_v)}

                    if isinstance(v, list) and len(v) <= keep_arrays_smaller_than:
                        #KEEP THE SMALL SAMPLES
                        new_v["samples"] = {
                            ("x%02d" % i): v
                            for i, v in enumerate(v)
                        }
                    else:
                        Log.note("Series {{path}} is not stored", {"path": path})

                r[k] = new_v
        elif isinstance(r, list):
            try:
                return stats(r)
            except Exception, e:
                for i, v in enumerate(r):
                    r[i] = summarize(path + "[]", v, keep_arrays_smaller_than)
        return r
    except Exception, e:
        Log.warning("Can not summarize: {{json}}", {"json": CNV.object2JSON(r)})


def stats(values):
    """
    RETURN LOTS OF AGGREGATES
    """
    z = Z_moment.new_instance(values)
    s = Struct()
    for k, v in z.dict:
        s[k] = v
    for k, v in z_moment2stats(z):
        s[k] = v
    s.max = Math.max(values)
    s.min = Math.min(values)
    s.median = dz2es.util.stats.median(values, simple=False)

    return Struct(samples=values, stats=stats)
