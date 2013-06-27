import itertools
from string import Template
import requests
from util.debug import D
from util.startup import startup
from util.cnv import CNV
from Transform import transform
from elasticsearch import ElasticSearch
from util.timer import Timer


def extract_from_datazilla(settings):

    #Retrieve revisions to iterate over
    url=settings.production.summary
    response = requests.get(url, params={
        "days_ago":30,
        "branches":"Places",
        "numdays":30
    })
    data=CNV.JSON2object(response.content)
    available_revisions=set(itertools.chain(*[v["revisions"] for k, v in data.items()]))
    D.println("Number of revisions in DZ: "+str(len(available_revisions)))

    
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
        "facets":{"revisions":{"terms":{"field":"test_build.revision","size":200000}}}
    })
    existing_revisions=set([t.term for t in existing_revisions.facets.revisions.terms])
    D.println("Number of revisions in ES: "+str(len(existing_revisions)))


    #ONLY PULL THE STUFF WE HAVE NOT SEEN
    es.set_refresh_interval(-1)
    new_revisions=available_revisions-existing_revisions
    D.println("${num_revisions} new revisions", {"num_revisions":len(new_revisions)})
    try:
        for revision in new_revisions:
            try:
                rawdata_url = Template(settings.production.detail+"/${branch}/${revision}/").substitute({
                    "branch":"Mozilla-Inbound",
                    "revision":revision
                })

                with Timer("read from DZ") as t:
                    content=requests.get(rawdata_url).content
                data=CNV.JSON2object(content)
                D.println(
                    "Add ${num_tests} tests for revision ${revision} (${size} bytes)", {
                    "num_tests":len(data),
                    "revision":revision,
                    "size":len(content)
                })

                if len(data)==0:
                    data=[
                        {"test_build":{"revision":revision}}
                    ]
                else:
                    for d in data: transform(d)

                with Timer("push to ES") as t:
                    es.load(data)
            except Exception, e:
                D.warning("Can not load data for revision ${revision}", {"revision":revision})
    finally:
        es.set_refresh_interval(1)




def reset(settings):
    ElasticSearch.delete_index(settings.elasticsearch)

    with open("test_schema.json") as f:
        schema=CNV.JSON2object(f.read(), flexible=True)
    ElasticSearch.create_index(settings.elasticsearch, schema)





settings=startup.read_settings()
#reset(settings)
extract_from_datazilla(settings)

