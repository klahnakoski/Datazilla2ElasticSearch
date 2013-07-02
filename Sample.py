
from util.debug import D
from util.cnv import CNV
from util.startup import startup
from elasticsearch import ElasticSearch


settings=startup.read_settings()

es=ElasticSearch(settings.elasticsearch)


with open("raw_json_blobs.tab", "r") as input_file:
    for i, line in enumerate(input_file):
        try:
            if len(line.strip())==0: continue
            col=line.split("\t")
            id=int(col[0])
            if id!=1608289: continue

            D.println(line)
            data=CNV.JSON2object(col[1]).json_blob
            data.datazilla_id=id
            es.load([data], "datazilla_id")
        except Exception, e:
            D.warning("can not process line:\n\t"+line, e)
