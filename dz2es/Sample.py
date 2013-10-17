from dz2es.Transform import DZ_to_ES
from util.logs import Log
from util.cnv import CNV
from util.startup import startup
from util.elasticsearch import ElasticSearch


settings = startup.read_settings()


es = ElasticSearch(settings.elasticsearch)

transformer = DZ_to_ES(settings.pushlog)

with open("Sample.tab", "w") as output_file:
    with open("raw_json_blobs.tab", "r") as input_file:
        for i, line in enumerate(input_file):
            try:
                if len(line.strip()) == 0:
                    continue
                col = line.split("\t")
                id = int(col[0])
                #                if id!=1608289: continue

                if i > 100:
                    break

                #                if 0.001<random(): continue
                Log.println(line)

                data = transformer.transform(id, CNV.JSON2object(col[1]))
                es.add([{"id": data.datazilla.id, "value": data}])
                output_file.write(line + "\n")
            except Exception, e:
                Log.warning("can not process line:\n\t" + line, e)
Log.println("Done Sample")