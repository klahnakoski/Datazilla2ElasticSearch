import os
import threading
import requests
import time
import thread
from util.basic import nvl
from util.debug import D
from util.startup import startup
from util.cnv import CNV
from Transform import transform
from elasticsearch import ElasticSearch
from util.timer import Timer







file_lock=threading.Lock()

## CONVERT DZ BLOB TO ES JSON
def etl(blob_id, es, settings):
    with Timer("read from DZ"):
        content = requests.get(settings.production.blob_url + "/" + str(blob_id)).content
    data = CNV.JSON2object(content)
    D.println("Add ${id} for revision ${revision} (${size} bytes)",
            {"id": blob_id, "revision": data.json_blob.test_build.revision,
             "size": len(content)})
    data=transform(data, datazilla_id=blob_id)
    with Timer("push to ES"):
        es.load([data], "datazilla.id")

    with file_lock:
        with open(settings.output_file, "a") as myfile:
            myfile.write(str(blob_id) + "\t" + content + "\n")


def etl_main_loop(es, VAL, existing_ids, settings):
    try:
        for blob_id in range(settings.production.min,
                             settings.production.max + settings.production.threads):
            if blob_id % settings.production.threads != VAL: continue
            if blob_id in existing_ids: continue
            try:
                etl(blob_id, es, settings)
            except Exception, e:
                D.warning("Can not load data for id ${id}", {"id": blob_id})
    finally:
        es.set_refresh_interval(1)


def extract_from_datazilla_using_id(settings):
    #FIND WHAT'S IN ES
    es=ElasticSearch(settings.elasticsearch)
    existing_ids=es.search({
        "query":{"filtered":{
            "query":{"match_all":{}},
            "filter":{"range":{"datazilla.id":{"gte":settings.production.min, "lt":settings.production.max+settings.production.threads}}}
        }},
        "from":0,
        "size":0,
        "sort":[],
        "facets":{"ids":{"terms":{"field":"datazilla.id","size":400000}}}
    })

    bad_ids=[]
    int_ids=set()
    for t in existing_ids.facets.ids.terms:
        try:
            int_ids.add(int(t.term))
        except Exception, e:
            bad_ids.append(t.term)
    existing_ids=int_ids
    D.println("Number of ids in ES: "+str(len(existing_ids)))
    D.println("BAD ids in ES: "+str(bad_ids))

    threads=[]
    for t in range(settings.production.threads):
        thread=threading.Thread(target=etl_main_loop, args=(es, t, existing_ids, settings))
#        thread.setDaemon(True)
        thread.start()
        threads.append(thread)

    try:
        for t in threads:
            t.join()
    finally:
        es.set_refresh_interval(1)


def reset(settings):
    try:
        ElasticSearch.delete_index(settings.elasticsearch)
    except Exception, e:
        pass

    with open("test_schema.json") as f:
        schema=CNV.JSON2object(f.read(), flexible=True)
    es=ElasticSearch.create_index(settings.elasticsearch, schema)

    es.set_refresh_interval(-1)

    if os.path.isfile(settings.output_file):
        with open(settings.output_file, "r") as myfile:
            for line in myfile:
                try:
                    if len(line.strip())==0: continue
                    col=line.split("\t")
                    id=col[0]
                    data=CNV.JSON2object(col[1])
                    data=transform(data, datazilla_id=id)

                    es.load([data], "datazilla.id")
                except Exception, e:
                    D.warning("Bad line (${length}bytes):\n\t${prefix}", {
                        "length":len(line),
                        "prefix":line[0:100]
                    })

    es.set_refresh_interval(1)
    time.sleep(2)


settings=startup.read_settings()
settings.production.threads=nvl(settings.production.threads, 1)
settings.output_file=nvl(settings.output_file, "raw_json_blobs.tab")


#reset(settings)
extract_from_datazilla_using_id(settings)

