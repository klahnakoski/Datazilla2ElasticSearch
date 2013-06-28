import sys
from util.cnv import CNV
from util.debug import D
from app import settings
from util.timer import Timer

with Timer("load pandas"):
    import pandas
    from pandas.core.frame import DataFrame

arrays = []

def arrays_add(path, r):

    try:
        if isinstance(r, dict):
            for k, v in [(k,v) for k,v in r.items()]:
                new_path=path+"["+CNV.value2quote(k)+"]"
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
parts=[0, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]

with open(settings.output_file, "r") as myfile:
    for i, line in enumerate(myfile):
#        if i>10: break
        col=line.split("\t")
        data=CNV.JSON2object(col[1]).json_blob
        arrays_add("", data)

df=DataFrame(arrays, columns=["path", "length", "count"])
length_dim=pandas.cut(df.length, parts, labels=["starting at "+("    "+str(p))[-4:] for p in parts[0:-1]], right=False)
summary=df.groupby(["path", length_dim]).agg({"count":sum})
#summary=summary.unstack(length_dim.labels)
#table=summary
table=summary.unstack("length")


#pandas.set_option("display.max_rows", 2000)
#pandas.set_option("display.max_columns",20)
#pandas.set_option('line_width', 40000)
#pandas.set_option('expand_frame_repr', True)
D.println(CNV.DataFrame2string(table))


#D.println("\n"+table.describe().to_string())




