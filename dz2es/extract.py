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
import functools
import requests
from dz2es.util.collections import MAX
from dz2es.util.env.files import File
from dz2es.util.env.profiles import Profiler
from dz2es.util.queries import Q
from dz2es.util.queries.es_query import ESQuery
from dz2es.util.struct import nvl, Null
from dz2es.util.env.logs import Log
from dz2es.util.env import startup
from dz2es.util.cnv import CNV
from dz2es.util.thread.threads import ThreadedQueue
from transform import DZ_to_ES
from dz2es.util.env.elasticsearch import ElasticSearch
from dz2es.util.times.timer import Timer
from dz2es.util.thread.multithread import Multithread


def etl(es, file_sink, settings, transformer, id):
    """
    PULL FROM DZ AND PUSH TO es AND file_sink
    """
    try:
        url = settings.production.blob_url + "/" + str(id)
        with Timer("read {{id}} from DZ", {"id": id}):
            content = requests.get(url, timeout=nvl(settings.production.timeout, 30)).content
    except Exception, e:
        Log.warning("Failure to read from {{url}}", {"url": url}, e)
        return False

    try:
        if content.startswith("Id not found"):
            Log.note("{{id}} not found", {"id": id})
            return False

        data = CNV.JSON2object(content.decode('utf-8'))
        content = CNV.object2JSON(data)  #ENSURE content HAS NO crlf

        if data.test_run_id:
            Log.println("Add {{id}} for revision {{revision}} ({{size}} bytes)",
                        {"id": id, "revision": data.json_blob.test_build.revision,
                         "size": len(content)})
            with Profiler("transform"):
                data = transformer.transform(id, data)

            es.extend({"value": d} for d in data)
            file_sink.add(str(id) + "\t" + content + "\n")

        return True
    except Exception, e:
        Log.warning("Failure to etl (content length={{length}})", {"length": len(content)}, e)
        return False


def get_existing_ids(es, settings):
    #FIND WHAT'S IN ES
    bad_ids = []
    int_ids = set()

    esq = ESQuery(es)

    max_id = esq.query({
        "from": es.settings.alias,
        "select": {"value": "datazilla.id", "aggregate": "max"}
    })

    interval_size = 200000
    for mini, maxi in Q.intervals(settings.production.min, max_id+interval_size, interval_size):
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
                settings.elasticsearch.index = current
                es = ElasticSearch(settings.elasticsearch)
        else:
            settings.elasticsearch.index = candidates.last()
            es = ElasticSearch(settings.elasticsearch)

    existing_ids = get_existing_ids(es, settings)
    max_existing_id = nvl(MAX(existing_ids), settings.production.min)
    holes = set(range(settings.production.min, max_existing_id)) - existing_ids
    missing_ids = set(range(settings.production.min, max_existing_id+nvl(settings.production.step, 200000))) - existing_ids
    Log.note("Number missing: {{num}}", {"num": len(missing_ids)})
    Log.note("Number in holes: {{num}}", {"num": len(holes)})
    #FASTER IF NO INDEXING IS ON
    es.set_refresh_interval(-1)

    #FILE IS FASTER THAN NETWORK
    if (len(holes) > 10000 or settings.args.scan_file or settings.args.restart) and File(settings.param.output_file).exists:
        #ASYNCH PUSH TO ES IN BLOCKS OF 1000
        with Timer("Scan file for missing ids"):
            with ThreadedQueue(es, size=1000) as json_for_es:
                num = 0
                for line in File(settings.param.output_file):
                    try:
                        if len(line.strip()) == 0:
                            continue
                        col = line.split("\t")
                        id = int(col[0])
                        # if id==3003529:
                        #     Log.debug()
                        if id < settings.production.min:
                            continue
                        if id in existing_ids:
                            continue

                        if num > settings.production.step:
                            return
                        num += 1

                        with Profiler("decode and transform"):
                            data = CNV.JSON2object(col[-1])
                            if data.test_run_id:
                                with Profiler("transform"):
                                    data = transformer.transform(id, data)
                                json_for_es.extend({"value": d} for d in data)
                                Log.note("Added {{id}} from file", {"id": id})

                                existing_ids.add(id)
                            else:
                                Log.note("Skipped {{id}} from file (no test_run_id)", {"id": id})
                                num -= 1

                    except Exception, e:
                        Log.warning("Bad line id={{id}} ({{length}}bytes):\n\t{{prefix}}", {
                            "id": id,
                            "length": len(CNV.object2JSON(line)),
                            "prefix": CNV.object2JSON(line)[0:130]
                        }, e)
        missing_ids = missing_ids - existing_ids

    #COPY MISSING DATA TO ES
    try:
        with ThreadedQueue(es, size=1000) as es_sink:
            with ThreadedQueue(File(settings.param.output_file), size=50) as file_sink:
                functions = [functools.partial(etl, *[es_sink, file_sink, settings, transformer]) for i in range(settings.production.threads)]

                num_not_found = 0
                with Multithread(functions) as many:
                    for result in many.execute([
                        {"id": id}
                        for id in Q.sort(missing_ids)[:nvl(settings.production.step, 200000):]
                    ]):
                        if not result:
                            num_not_found += 1
                            if num_not_found > 100:
                                many.inbound.pop_all()  # CLEAR THE QUEUE OF OTHER WORK
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
    if settings.args.no_restart:
        Log.error("reset not allowed")
    schema_json = File(settings.param.schema_file).read()
    schema = CNV.JSON2object(schema_json, {"type": settings.elasticsearch.type}, flexible=True)

    # USE UNIQUE NAME EACH TIME RUN
    if settings.elasticsearch.alias == Null:
        settings.elasticsearch.alias = settings.elasticsearch.index
    settings.elasticsearch.index = ElasticSearch.proto_name(settings.elasticsearch.alias)

    es = ElasticSearch.create_index(settings.elasticsearch, schema, limit_replicas=True)
    return es


def main():
    try:
        settings = startup.read_settings(defs=[{
            "name": ["--no_restart", "--no_reset", "--no_redo", "--norestart", "--noreset", "--noredo"],
            "help": "do not allow creation of new index (for debugging rouge resets)",
            "action": "store_true",
            "dest": "no_restart"
        }, {
            "name": ["--restart", "--reset", "--redo"],
            "help": "force a reprocessing of all data",
            "action": "store_true",
            "dest": "restart"
        }, {
            "name": ["--file", "--scan_file", "--scanfile", "--use_file", "--usefile"],
            "help": "scan file for missing ids",
            "action": "store_true",
            "dest": "scan_file"
        }, {
            "name": ["--nofile", "--no_file", "--no-file"],
            "help": "do not scan file for missing ids",
            "action": "store_false",
            "dest": "scan_file"
        }])
        Log.start(settings.debug)

        with startup.SingleInstance(flavor_id=settings.args.filename):
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
