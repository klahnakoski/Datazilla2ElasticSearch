from util.cnv import CNV
from util.query import Q
from util.maths import is_number
from util.stats import Moments

from util.db import DB
from elasticsearch import ElasticSearch
from util.startup import startup




class d2e:

    def __init__(self, dz, es):
        self.dz=dz
        self.es=es


    def run(self):
        min_id=0
        limit=1000

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
            r.json=CNV.JSON2object(r.json_blob)
            r.json_blob=None

            d2e.summarize(r)


    # RESPONSIBLE FOR CONVERTING LONG ARRAYS OF NUMBERS TO THEIR REPRESENTATIVE
    # MOMENTS
    @staticmethod
    def summarize(r):
        for k, v in r.copy():
            try:
                if isinstance(v, list) and all([is_number(n) for n in v]):
                    r[k+"_moments"]=Moments.new_instance(v)
                r[k]=None
            finally:
                pass








# READ SETTINGS
settings=startup.read_settings()

# BUILD SOURCE AND SINK
dz=DB(**settings.datazilla)
es=ElasticSearch(**settings.elasticsearch)

# RUN
converter=d2e(dz, es)
converter.run()
