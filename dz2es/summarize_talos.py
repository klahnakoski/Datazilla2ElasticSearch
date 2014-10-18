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
from pyLibrary.maths import Math
from pyLibrary.env import startup
from pyLibrary.cnv import CNV
from pyLibrary.env.logs import Log
from pyLibrary.times.timer import Timer


with Timer("load pandas"):
    import pandas
    from pandas.core.frame import DataFrame

MINIMUM_DATE = CNV.string2datetime("20130720", "%Y%m%d")
MINIMUM_ID = 0
parts = [0, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000]

arrays = []

#TRAVERSE THE JSON GRAPH AND REPORT THE float() ARRAY POPULATIONS
def arrays_add(id, path, r):
    try:
        if isinstance(r, dict):
            for k, v in [(k, v) for k, v in r.items()]:
                new_path = path + "[" + k + "]"
                arrays_add(id, new_path, v)
        elif isinstance(r, list):
            try:
                values = r.map(float)
                arrays.append([id, path, len(values), 1])
            except Exception, e:
                for i, v in enumerate(r):
                    r[i] = arrays_add(id, path + "[" + str(i) + "]", v)
                #        return r
    except Exception, e:
        Log.warning("Can not summarize: {{json}}", {"json": CNV.object2JSON(r)})


settings = startup.read_settings()
Log.settings(settings.debug)
all = set()

with open(settings.output_file, "r") as input_file:
    with open("good_talos.tab", "w") as output_file:
        for line in input_file:
            try:
                if len(line.strip()) == 0:
                    continue

                col = line.split("\t")
                id = int(col[0])
                if id < MINIMUM_ID:
                    continue

                json = col[1]
                if Math.is_number(json):
                    json = col[2]
                data = CNV.JSON2object(json).json_blob
                date = CNV.unix2datetime(data.testrun.date)

                if id % 1000 == 0:
                    Log.println("loading id " + str(id) + " date: " + CNV.datetime2string(date, "%Y-%m-%d %H:%M:%S"))

                if date < MINIMUM_DATE:
                    continue

                if id in all:
                    continue
                all.add(id)

                arrays_add(id, "[" + data.test_build.branch + "][" + data.testrun.suite + "]", data)
                output_file.write(str(id) + "\t" + json)
            except Exception, e:
                Log.warning("can not process line:\n\t" + line, e)

        smallest = min(*all)
        Log.println("First id >= date: {{min}}", {"min": smallest})

df = DataFrame(arrays, columns=["id", "path", "length", "count"])
colNames = [str(p) + " to " + str(parts[i + 1] - 1) for i, p in enumerate(parts[0:-1])]

# http://pandas.pydata.org/pandas-docs/stable/groupby.html#na-group-handling
length_dim = pandas.cut(df.length, parts, labels=colNames, right=False)
summary = df.groupby(["path", length_dim], sort=False).size()
#summary=summary.reindex(length_dim, level="length")
table = summary.unstack("length")
s = CNV.DataFrame2string(table)#, columns=colNames)
Log.println("\n" + s)
with open("talos_big_array_summary.tab", "w") as output_file:
    output_file.write(s)

sum2 = df.groupby(["path", "length"]).size()
tab2 = sum2.unstack("length")
s = CNV.DataFrame2string(tab2)#, columns=colNames)
Log.println("\n" + s)
with open("talos_every_population.tab", "w") as output_file:
    output_file.write(s)

biggest = df[df.length == 63000]
s = CNV.DataFrame2string(biggest)
Log.println("\n" + s)
with open("talos_biggest.tab", "w") as output_file:
    output_file.write(s)








