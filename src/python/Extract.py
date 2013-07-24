from datetime import datetime
import functools
import os
import threading
import requests
import time
from util.basic import nvl
from util.debug import D
from util.startup import startup
from util.cnv import CNV
from Transform import DZ_to_ES
from elasticsearch import ElasticSearch
from util.timer import Timer
from util.multithread import Multithread


file_lock=threading.Lock()

## CONVERT DZ BLOB TO ES JSON
def etl(es, settings, id):
    try:
        with Timer("read from DZ"):
            content = requests.get(settings.production.blob_url + "/" + str(id)).content
        data = CNV.JSON2object(content)
        D.println("Add ${id} for revision ${revision} (${size} bytes)",
                {"id": id, "revision": data.json_blob.test_build.revision,
                 "size": len(content)})
        data=transformer.transform(id, data)
        with Timer("push to ES"):
            es.load([data], "datazilla.id")

        with file_lock:
            with open(settings.output_file, "a") as myfile:
                myfile.write(str(id) + "\t" + content + "\n")
        return True
    except Exception, e:
        D.warning("Failure to etl", e)
        return False
    

def get_exiting_ids(es, settings):
    #FIND WHAT'S IN ES
    existing_ids = es.search({
        "query": {
            "filtered": {
                "query": {"match_all": {}},
                "filter": {
                    "range":{"datazilla.id":{
                        "gte": settings.production.min,
                        "lt": settings.production.max + settings.production.threads
                    }}
                }
            }
        },
        "from": 0, 
        "size": 0,
        "sort": [],
        "facets": {
            "ids": {"terms": {"field": "datazilla.id", "size": 400000}}
        }
    })
    bad_ids = []
    int_ids = set()
    for t in existing_ids.facets.ids.terms:
        try:
            int_ids.add(int(t.term))
        except Exception, e:
            bad_ids.append(t.term)
    existing_ids = int_ids
    D.println("Number of ids in ES: " + str(len(existing_ids)))
    D.println("BAD ids in ES: " + str(bad_ids))
    return existing_ids


def extract_from_datazilla_using_id(settings):
    es=ElasticSearch(settings.elasticsearch)

    #SETUP NEW INDEX
    if settings.elasticsearch.alias is None: settings.elasticsearch.alias=settings.elasticsearch.index
    if settings.elasticsearch.alias==settings.elasticsearch.index:
        possible_indexes=[a.index for a in es.get_aliases() if a.alias==settings.elasticsearch.alias]
        if len(possible_indexes)==0:
            D.error("expecting an index with '"+settings.elasticsearch.alias+"' as alias")
        settings.elasticsearch.index=possible_indexes[0]


    #COPY MISSING DATA TO ES
    try:
        existing_ids = get_exiting_ids(es, settings)
        missing_ids=set(range(settings.production.min, settings.production.max))-existing_ids
        functions=[functools.partial(etl, *[es, settings]) for i in range(settings.production.threads)]

        num_not_found=0
        with Multithread(functions) as many:
            for result in many.execute([{"id":id} for id in missing_ids]):
                if not result:
                    num_not_found+=1
                    if num_not_found>100:
                        many.stop()
                        break
                else:
                    num_not_found=0
    except (KeyboardInterrupt, SystemExit):
        D.println("Shutdow Started, please be patient")
    except Exception, e:
        D.error("Unusual shutdown!", e)

    #FINISH ES SETUP SO IT CAN BE QUERIED
    es.set_refresh_interval(1)
    es.delete_all_but(settings.elasticsearch.alias, settings.elasticsearch.index)
    es.add_alias(settings.elasticsearch.alias)




def reset(settings):
    with open("test_schema.json") as f:
        schema=CNV.JSON2object(f.read(), {"type":settings.elasticsearch.type}, flexible=True)

    # USE UNIQUE NAME EACH TIME RUN
    if settings.elasticsearch.alias is None: settings.elasticsearch.alias=settings.elasticsearch.index
    settings.elasticsearch.index=settings.elasticsearch.alias+CNV.datetime2string(datetime.utcnow(), "%Y%m%d_%H%M%S")
    es=ElasticSearch.create_index(settings.elasticsearch, schema)

    es.set_refresh_interval(-1)

    if os.path.isfile(settings.output_file):
        with open(settings.output_file, "r") as myfile:
            for line in myfile:
                try:
                    if len(line.strip())==0: continue
                    col=line.split("\t")
                    id=int(col[0])
                    if id<settings.production.min or settings.production.max<=id: continue
                    data=CNV.JSON2object(col[1])
                    data=transformer.transform(id, data)
                    es.load([data], "datazilla.id")
                except Exception, e:
                     D.warning("Bad line (${length}bytes):\n\t${prefix}", {
                         "length":len(CNV.object2JSON(line)),
                         "prefix":CNV.object2JSON(line)[0:130]
                     }, e)

    es.set_refresh_interval(1)
    time.sleep(2)


settings=startup.read_settings()
settings.production.threads=nvl(settings.production.threads, 1)
settings.output_file=nvl(settings.output_file, "raw_json_blobs.tab")



transformer=DZ_to_ES(settings.pushlog)
#RESET ONLY IF NEW Transform IS USED
reset(settings)
extract_from_datazilla_using_id(settings)

