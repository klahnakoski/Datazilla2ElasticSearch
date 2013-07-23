from math import floor, ceil
from string import replace
from util.cnv import CNV
from util.db import DB
from util.debug import D
from util.query import Q
from util.stats import Z_moment


DEBUG=False

class DZ_to_ES():


    def __init__(self, pushlog_settings):
        self.pushlog=get_pushlog(pushlog_settings)



    # A SIMPLE TRANSFORM OF DATA:  I WOULD ALSO LIKE TO ADD DIMENSIONAL TYPE INFORMATION
    # WHICH WOULD GIVE DEAR READER A BETTER FEEL FOR THE TOTALITY OF THIS DATA
    # BUT THEN AGAIN, SIMPLE IS BETTER, YES?
    def transform(self, id, datazilla, keep_arrays_smaller_than=25):
        r=datazilla.json_blob

        #ADD DATAZILLA MARKUP
        r.datazilla={
            "id":id,
            "date_loaded":datazilla.date_loaded,
            "error_flag":datazilla.error_flag,
            "test_run_id":datazilla.test_run_id,
            "processed_flag":datazilla.processed_flag,
            "error_msg":datazilla.error_msg
        }


        new_results={}
        for k,v in r.results.items():
            k=replace(k, ".", "_dot_")
            if len(v)<=keep_arrays_smaller_than:
                new_results[k]=v
    #        else:
    #            try:
    #                new_results[k]={"moments":Z_moment.new_instance(v).dict}
    #            except Exception, e:
    #                new_results[k]={"moments":Z_moment.new_instance(v).dict}
    #                D.error("can not reduce series to moments", e)
        r.results=new_results

        #CONVERT FROM <name>:<samples> TO {"name":<name>, "samples":<samples>}
        #USING stack() WOULD BE CLEARER, BUT DOES NOT HANDLE THE TOO-LARGE SEQUENCES
    #    r.results=Q.stack([r.results], column="name")
    #    r.results=[{
    #        "name":k,
    #        "moments": Z_moment.new_instance(v).dict,
    #        "samples": (dict(("s"+right("00"+str(i), 2), s) for i, s in enumerate(v)) if len(v)<=keep_arrays_smaller_than else None)
    #    } for k,v in r.results.items()]

        #CONVERT UNIX TIMESTAMP TO MILLISECOND TIMESTAMP
        r.testrun.date*=1000

        #COLAPSE THESE TO SIMPLE MOMENTS
        if r.results_aux is not None:
            r.results_aux.responsivness     ={"moments":Z_moment.new_instance(r.results_aux.responsivness   ).dict}
            r.results_aux["Private bytes"]  ={"moments":Z_moment.new_instance(r.results_aux["Private bytes"]).dict}
            r.results_aux.Main_RSS          ={"moments":Z_moment.new_instance(r.results_aux.Main_RSS        ).dict}
            r.results_aux.shutdown          ={"moments":Z_moment.new_instance(r.results_aux.shutdown        ).dict}


        if r.results_xperf is not None:
            r.results_xperf.mainthread_writebytes=[{"path":i[1], "value":i[0]} for i in r.results_xperf.mainthread_writebytes]
            r.results_xperf.mainthread_readcount=[{"path":i[1], "value":i[0]} for i in r.results_xperf.mainthread_readcount]
            r.results_xperf.mainthread_writecount=[{"path":i[1], "value":i[0]} for i in r.results_xperf.mainthread_writecount]
            r.results_xperf.mainthread_readbytes=[{"path":i[1], "value":i[0]} for i in r.results_xperf.mainthread_readbytes]
    #    summarize(r.dict, keep_arrays_smaller_than)

        #ADD PUSH LOG INFO
        try:
            possible_dates=self.pushlog[r.test_build.branch][r.test_build.revision]
            r.test_build.push_date=possible_dates[0].date
        except Exception, e:
            D.warning(r.test_build.branch+"@"+r.test_build.revision+" has no pushlog", e)
        
        return r



def get_pushlog(settings):
    with DB(settings) as db:
        all_pushlogs=db.query("""
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

        return Q.index(all_pushlogs, ["branch", "revision"])







# RESPONSIBLE FOR CONVERTING LONG ARRAYS OF NUMBERS TO THEIR REPRESENTATIVE
# MOMENTS
def summarize(r, keep_arrays_smaller_than=25):

    try:
        if isinstance(r, dict):
            for k, v in [(k,v) for k,v in r.items()]:
                new_v=summarize(v)
                if isinstance(new_v, Z_moment):
                    #CONVERT MOMENTS' TUPLE TO NAMED HASH (FOR EASIER ES INDEXING)
                    new_v={"moment":new_v.dict}


                    if isinstance(v, list) and len(v)<=keep_arrays_smaller_than:
                        #KEEP THE SMALL SAMPLES
                        new_v["samples"]=dict([("x"+("0"+str(i))[-2:],v) for i, v in enumerate(v)])

                r[k]=new_v
        elif isinstance(r, list):
            try:
                bottom=0.0
                top=0.9

                ## keep_middle - THE PROPORTION [0..1] OF VALUES TO KEEP, EXTREMES ARE REJECTED
                values=[float(v) for v in r]

                length=len(values)
                min=int(floor(length*bottom))
                max=int(ceil(length*top))
                values=sorted(values)[min:max]
                if DEBUG: D.println("${num} of ${total} used in moment", {"num":len(values), "total":length})

                return Z_moment.new_instance(values)
            except Exception, e:
                for i, v in enumerate(r):
                    r[i]=summarize(v)
        return r
    except Exception, e:
        D.warning("Can not summarize: ${json}", {"json":CNV.object2JSON(r)})


