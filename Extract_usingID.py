import itertools
from string import Template
import requests
from util.debug import D
from util.startup import startup
from util.cnv import CNV
from Transform import transform
from elasticsearch import ElasticSearch
from util.timer import Timer


def extract_from_datazilla_using_id(settings):

    #FIND WHAT'S IN ES
    es=ElasticSearch(settings.elasticsearch)
    existing_revisions=es.search({
        "query":{"filtered":{
            "query":{"match_all":{}},
            "filter":{"script":{"script":"true"}}
        }},
        "from":0,
        "size":0,
        "sort":[],
        "facets":{"ids":{"terms":{"field":"datazilla_id","size":200000}}}
    })
    existing_ids=set([t.term for t in existing_revisions.facets.ids.terms])
    D.println("Number of ids in ES: "+str(len(existing_ids)))


    try:

        for blob_id in range(settings.production.min, settings.production.max+1):
            if blob_id in existing_ids: continue

            try:
                with Timer("read from DZ") as t:
                    content=requests.get(settings.production.json_blob+"/"+str(blob_id)).content

                data=CNV.JSON2object(content).json_blob
                D.println(
                    "Add ${id} for revision ${revision} (${size} bytes)", {
                    "id":blob_id,
                    "revision":data.test_build.revision,
                    "size":len(content)
                })

                if len(data)==0:
                    data=[
                        {"datazilla_id":blob_id}
                    ]
                else:
                    for d in data: transform(d, datazilla_id=blob_id)

                with Timer("push to ES") as t:
                    es.load(data, "datazilla_id")
            except Exception, e:
                D.warning("Can not load data for id ${id}", {"id":blob_id})
    finally:
        es.set_refresh_interval(1)




def reset(settings):
    ElasticSearch.delete_index(settings.elasticsearch)

    with open("test_schema.json") as f:
        schema=CNV.JSON2object(f.read(), flexible=True)
    ElasticSearch.create_index(settings.elasticsearch, schema)





settings=startup.read_settings()
reset(settings)
extract_from_datazilla_using_id(settings)

