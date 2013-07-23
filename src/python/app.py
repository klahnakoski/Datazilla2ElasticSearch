from datetime import datetime
from math import ceil, floor
from util.cnv import CNV
from util.query import Q
from util.stats import Z_moment

from util.db import DB
from elasticsearch import ElasticSearch
from util.debug import D
from util.startup import startup



DEBUG = False


class d2e:

    def __init__(self, dz, es):
        self.dz=dz
        self.es=es


    def run(self):
        min_id=0
        limit=100

        while True:
            records=self.extract(min_id, limit)
            self.transform(records)

            #            self.es.load(records)

            if len(records)<limit: break
            min_id=max(*Q.select(records, "id"))


    def extract(self, min_id, limit):
        ## GRAB A BLOCK OF DATA
        return self.dz.query("""
            SELECT
                id,
                test_run_id,
                date_loaded,
                processed_flag,
                error_flag,
                error_msg,
                json_blob,
                worker_id
            FROM
                objectstore
            WHERE
                id>${min_id} AND
                instr(json_blob, "yelp.com")>0 and
                instr(json_blob, "62d955cfe160")>0 AND
                instr(json_blob, "tp5o")>0
            ORDER BY
                id
            LIMIT
                ${limit}
            """,{
                "min_id":min_id,
                "limit":limit
        })
#
#                instr(json_blob, "1366212899")>0
#                (instr(json_blob, "136324")>0 or instr(json_blob, "136325")>0)


    def transform(self, records):
        for r in records:
            json = r.json_blob
            r.json = d2e.summarize(CNV.JSON2object(json).dict)

    #            mintime=CNV.datetime2unix(datetime(2013, 03, 14, 04, 0, 0))*1000
    #            maxtime=CNV.datetime2unix(datetime(2013, 03, 14, 05, 0, 0))*1000
    #
    #            if mintime<=r.json.testrun.date<maxtime:
    #            D.println(str(r.json.results.amazon_dot_com))
            D.println(str(r.json))
    #            D.println(str(r.json.results_aux))

            r.json_blob = None




# READ SETTINGS
settings=startup.read_settings()




# BUILD SOURCE AND SINK
with DB(settings.datazilla) as dz:
    es=ElasticSearch(settings.elasticsearch)
    with open("test_schema.json") as f:
        schema=CNV.JSON2object(f.read(), flexible=True)
#    ElasticSearch.delete_index(settings.elasticsearch)
#    ElasticSearch.create_index(settings.elasticsearch, schema)
#    es.set_refresh_interval(-1)

    # RUN
    converter=d2e(dz, es)
    converter.run()
#    es.set_refresh_interval(1)