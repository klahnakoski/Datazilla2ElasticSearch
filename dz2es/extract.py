################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################


from datetime import datetime
import functools
import requests
from dz2es.util.files import File
from dz2es.util.maths import Math
from dz2es.util.queries import Q
from dz2es.util.struct import nvl, Null
from dz2es.util.logs import Log
from dz2es.util import startup
from dz2es.util.cnv import CNV
from dz2es.util.threads import ThreadedQueue
from transform import DZ_to_ES
from dz2es.util.elasticsearch import ElasticSearch
from dz2es.util.timer import Timer
from dz2es.util.multithread import Multithread



def etl(es, file_sink,  settings, transformer, id):
    """
    PULL FROM DZ AND PUSH TO es AND file_sink
    """
    try:
        url = settings.production.blob_url + "/" + str(id)
        with Timer("read from DZ"):
            content = requests.get(url).content
    except Exception, e:
        Log.warning("Failure to read from {{url}}", {"url": url}, e)
        return False

    try:
        if content.startswith("Id not found"):
            Log.note("{{id}} not found", {"id":id})
            return False

        data = CNV.JSON2object(content)
        content = CNV.object2JSON(data)  #ENSURE content HAS NO crlf
        Log.println("Add {{id}} for revision {{revision}} ({{size}} bytes)",
                    {"id": id, "revision": data.json_blob.test_build.revision,
                     "size": len(content)})
        data = transformer.transform(id, data)

        es.add({"id": data.datazilla.id, "value": data})
        file_sink.add(str(id) + "\t" + content + "\n")
        return True
    except Exception, e:
        Log.warning("Failure to etl (content length={{length}})", {"length": len(content)}, e)
        return False


def get_existing_ids(es, settings):
    #FIND WHAT'S IN ES
    bad_ids = []
    int_ids = set()

    interval_size = 400000
    for mini, maxi in Q.range(settings.production.min, settings.production.max, interval_size):
        existing_ids = es.search({
            "query": {
                "filtered": {
                    "query": {"match_all": {}},
                    "filter": {
                        "range": {"datazilla.id": {
                            "gte": mini,
                            "lt": maxi
                        }}
                    }
                }
            },
            "from": 0,
            "size": 0,
            "sort": [],
            "facets": {
                "ids": {"terms": {"field": "datazilla.id", "size": interval_size}}
            }
        })

        for t in existing_ids.facets.ids.terms:
            try:
                int_ids.add(int(t.term))
            except Exception, e:
                bad_ids.append(t.term)

    existing_ids = int_ids
    Log.println("Number of ids in ES: " + str(len(existing_ids)))
    Log.println("BAD ids in ES: " + str(bad_ids))
    return existing_ids


def extract_from_datazilla_using_id(settings, transformer):
    es = ElasticSearch(settings.elasticsearch)

    #FIND SPECIFIC INDEX
    if settings.elasticsearch.alias == Null:
        settings.elasticsearch.alias = settings.elasticsearch.index
    if settings.elasticsearch.alias == settings.elasticsearch.index:
        candidates = es.get_proto(settings.elasticsearch.alias)
        current = es.get_index(settings.elasticsearch.alias)
        if not candidates:
            if not current or settings.args.restart:
                settings.args.restart = True
                es = reset(settings)
            else:
                es = ElasticSearch(settings.elasticsearch)
        else:
            settings.elasticsearch.index = candidates.last()
            es = ElasticSearch(settings.elasticsearch)

    existing_ids = get_existing_ids(es, settings)
    holes = set(range(settings.production.min, nvl(Math.max(existing_ids), settings.production.min))) - existing_ids
    missing_ids = set(range(settings.production.min, settings.production.max)) - existing_ids
    Log.note("Number missing: {{num}}", {"num": len(missing_ids)})
    Log.note("Number in holes: {{num}}", {"num": len(holes)})
    #FASTER IF NO INDEXING IS ON
    es.set_refresh_interval(-1)

    #FILE IS FASTER THAN NETWORK
    if (len(holes) > 10000 or settings.args.scan_file or settings.args.restart) and File(settings.param.output_file).exists:
        #ASYNCH PUSH TO ES IN BLOCKS OF 1000
        with Timer("Scan file for missing ids"):
            with ThreadedQueue(es, size=5000) as json_for_es:
                for line in File(settings.param.output_file):
                    try:
                        if len(line.strip()) == 0:
                            continue
                        col = line.split("\t")
                        id = int(col[0])
                        if id < settings.production.min or settings.production.max <= id:
                            continue
                        if id in existing_ids:
                            continue

                        data = CNV.JSON2object(col[-1])
                        data = transformer.transform(id, data)
                        json_for_es.add({"id": data.datazilla.id, "value": data})
                        Log.note("Added {{id}} from file", {"id": data.datazilla.id})

                        existing_ids.add(id)
                    except Exception, e:
                        Log.warning("Bad line id={{id}} ({{length}}bytes):\n\t{{prefix}}", {
                            "id": id,
                            "length": len(CNV.object2JSON(line)),
                            "prefix": CNV.object2JSON(line)[0:130]
                        }, e)
        missing_ids = missing_ids - existing_ids

    #COPY MISSING DATA TO ES
    try:
        with ThreadedQueue(File(settings.param.output_file), size=50) as file_sink:
            functions = [functools.partial(etl, *[es, file_sink, settings, transformer]) for i in range(settings.production.threads)]

            num_not_found = 0
            with Multithread(functions) as many:
                for result in many.execute([
                    {"id": id}
                    for id in missing_ids - existing_ids
                ]):
                    if not result:
                        num_not_found += 1
                        if num_not_found > 100:
                            many.stop()
                            break
                    else:
                        num_not_found = 0
    except (KeyboardInterrupt, SystemExit):
        Log.println("Shutdown Started, please be patient")
    except Exception, e:
        Log.error("Unusual shutdown!", e)

    #FINISH ES SETUP SO IT CAN BE QUERIED
    es.set_refresh_interval(1)
    es.delete_all_but(settings.elasticsearch.alias, settings.elasticsearch.index)
    es.add_alias(settings.elasticsearch.alias)


def reset(settings):
    Log.error("reset not allowed");
    schema_json = File(settings.param.schema_file).read()
    schema = CNV.JSON2object(schema_json, {"type": settings.elasticsearch.type}, flexible=True)

    # USE UNIQUE NAME EACH TIME RUN
    if settings.elasticsearch.alias == Null:
        settings.elasticsearch.alias = settings.elasticsearch.index
    settings.elasticsearch.index = ElasticSearch.proto_name(settings.elasticsearch.alias)
    es = ElasticSearch.create_index(settings.elasticsearch, schema)
    return es


def main():
    try:
        settings = startup.read_settings(defs=[{
            "name": ["--restart", "--reset", "--redo"],
            "help": "force a reprocessing of all data",
            "action": "store_true",
            "dest": "restart"
        },{
            "name": ["--file", "--scan_file", "--scanfile"],
            "help": "scan file for missing ids",
            "action": "store_true",
            "dest": "scan_file"
        }])
        Log.start(settings.debug)

        settings.production.threads = nvl(settings.production.threads, 1)
        settings.param.output_file = nvl(settings.param.output_file, "./results/raw_json_blobs.tab")

        transformer = DZ_to_ES(settings.pushlog)

        #RESET ONLY IF NEW Transform IS USED
        if settings.args.restart:
            reset(settings)
        extract_from_datazilla_using_id(settings, transformer)
    finally:
        Log.stop()


main()
