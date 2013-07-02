from util.startup import startup
from util.cnv import CNV
from util.debug import D
from util.timer import Timer

with Timer("load pandas"):
    import pandas
    from pandas.core.frame import DataFrame

arrays = []

def arrays_add(path, r):

    try:
        if isinstance(r, dict):
            for k, v in [(k,v) for k,v in r.items()]:
                new_path=path+"["+k+"]"
                arrays_add(new_path, v)
        elif isinstance(r, list):
            try:
                values=[float(v) for v in r]
                arrays.append([path, len(values), 1])
            except Exception, e:
                for i, v in enumerate(r):
                    r[i]=arrays_add(path+"["+str(i)+"]", v)
#        return r
    except Exception, e:
        D.warning("Can not summarize: ${json}", {"json":CNV.object2JSON(r)})






#startup()
#THIS IS WHAT I WOULD LIKE TO DO
#data=Q({
#    "from":{"type":"tab", "name":settings.output_file, "field_seperator":"\t", "line_seperator":"\n", "column_names":False},
#    "select":[
#        {"name":"id",   "value":"int(column[0])", "type":"integer"},
#        {"name":"json", "value":"CNV.JSON2object(column[1]).json_blob", "type":"object"}
#    ]
#})
#
#sum=Q({
#    "from":data,
#    "execute":"arrays_add("", json)"
#})
#
#df=Q({
#    "from":arrays,
#    "select":{"name":"num", "value":"1", "aggregate":"count"},
#    "edges":[
#        {"name":"path", "value":"column[0]"},
#        {"name":"length", "value":"column[1]", "domain":Q.domain([0, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000])}
#    ]
#})

#THIS IS WHAT I MUST DO
settings=startup.read_settings()
parts=[0, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000]
all=set()


with open(settings.output_file, "r") as input_file:
    with open("good_talos.tab", "w") as output_file:
        for i, line in enumerate(input_file):
            try:
                if i>100: break
                if i % 1000==0: D.println("loading line "+str(i))
                if i in all: continue
                all.add(i)

                col=line.split("\t")
                data=CNV.JSON2object(col[1]).json_blob
                arrays_add("["+data.testrun.suite+"]", data)
                output_file.write(line+"\n")
            except Exception, e:
                D.warning("can not process line:\n\t"+line, e)

df=DataFrame(arrays, columns=["path", "length", "count"])
length_dim=pandas.cut(df.length, parts, labels=[("     "+str(p))[-5:]+" to "+str(parts[i+1]-1) for i,p in enumerate(parts[0:-1])], right=False)
summary=df.groupby(["path", length_dim]).size()
table=summary.unstack("length")
s=CNV.DataFrame2string(table)
D.println("\n"+s)
with open("talos_summary1.tab", "w") as output_file:
    output_file.write(s)

sum2=df.groupby(["path", "length"]).size()
tab2=sum2.unstack("length")
s=CNV.DataFrame2string(tab2)
D.println("\n"+s)
with open("talos_summary2.tab", "w") as output_file:
    output_file.write(s)





