from random import random
from util.debug import D
from util.cnv import CNV
from util.startup import startup
from elasticsearch import ElasticSearch


settings=startup.read_settings()

#D.add_log(Log.new_instance(file=))

es=ElasticSearch(settings.elasticsearch)

with open("Sample.tab", "w") as output_file:
    with open("raw_json_blobs.tab", "r") as input_file:
        for i, line in enumerate(input_file):
            try:
                if len(line.strip())==0: continue
                col=line.split("\t")
                id=int(col[0])
#                if id!=1608289: continue

                if i>100: break

#                if 0.001<random(): continue
                D.println(line)

                data=CNV.JSON2object(col[1]).json_blob
                data.datazilla.id=id
                es.load([data], "datazilla.id")
                output_file.write(line+"\n")
            except Exception, e:
                D.warning("can not process line:\n\t"+line, e)
D.println("Done Sample")