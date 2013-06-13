from util.cnv import CNV
from util.map import Map
from util.query import Q
from util.stats import Moments

from util.db import DB
from elasticsearch import ElasticSearch
from util.debug import D
from util.startup import startup




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
            self.es.load(records)

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
                id>${min_id}
            LIMIT
                ${limit}
            """,{
                "min_id":min_id,
                "limit":limit
        })


    def transform(self, records):
        for r in records:
            r.json= d2e.summarize(CNV.JSON2object(r.json_blob).dict)
            r.json_blob=None


    # RESPONSIBLE FOR CONVERTING LONG ARRAYS OF NUMBERS TO THEIR REPRESENTATIVE
    # MOMENTS
    @staticmethod
    def summarize(r):
        try:
            if isinstance(r, dict):
                for k, v in [(k,v) for k,v in r.items()]:
                    new_v=d2e.summarize(v)
                    if isinstance(new_v, Moments):
                        #CONVERT MOMENTS' TUPLE TO NAMED HASH (FOR EASIER ES INDEXING)
                        new_v={"moments":dict([("s"+str(i), m) for i, m in enumerate(new_v.tuple)])}

                    #CONVERT UNIX TIMESTAMP TO MILLISECOND TIMESTAMP
                    if k in ["date", "date_loaded"]: new_v*=1000

                    #REMOVE DOT FROM NAME, SO EASIER TO QUERY
                    new_k=k.replace(".", "_dot_")
                    r[k]=None
                    r[new_k]=new_v
#                    r[k]=new_v
            elif isinstance(r, list):
                try:
                    return Moments.new_instance(r)
                except Exception, e:
                    for i, v in enumerate(r):
                        r[i]=d2e.summarize(v)
            return r
        except Exception, e:
            D.warning("Can not summarize: ${json}", {"json":CNV.object2JSON(r)})











# READ SETTINGS
settings=startup.read_settings()




# BUILD SOURCE AND SINK
with DB(settings.datazilla) as dz:
    es=ElasticSearch(settings.elasticsearch)
    with open("test_schema.json") as f:
        schema=CNV.JSON2object(f.read(), flexible=True)
    ElasticSearch.delete_index(settings.elasticsearch)
    ElasticSearch.create_index(settings.elasticsearch, schema)
    es.set_refresh_interval(-1)

    # RUN
    converter=d2e(dz, es)
    converter.run()
    es.set_refresh_interval(1)