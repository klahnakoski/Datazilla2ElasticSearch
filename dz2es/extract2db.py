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
from pyLibrary.cnv import CNV
from pyLibrary.sql.db import DB
from pyLibrary.env.files import File
from pyLibrary.env.logs import Log
from pyLibrary.env import startup
from pyLibrary.struct import nvl, Struct
from pyLibrary.thread.threads import ThreadedQueue


def file2db(db, table_name, filename):

    def extend(d):
        try:
            db.insert_list(table_name, d)
            db.flush()
            Log.note("added {{num}} records", {"num":len(d)})
        except Exception, e:
            Log.warning("Can not inert into database", e)

    db_queue = Struct(extend=extend)

    with ThreadedQueue(db_queue, 100) as records_for_db:
        added = set()

        for line in File(filename).iter():
            try:
                if len(line.strip()) == 0: continue
                col = line.split("\t")
                id = int(col[0])
                if id in added:
                    continue
                added.add(id)

                data = CNV.JSON2object(col[1])
                records_for_db.add({
                    "id": nvl(data.test_run_id, id),
                    "branch": data.json_blob.test_build.branch,
                    "name": data.json_blob.test_build.name,
                    "version": data.json_blob.test_build.version,
                    "suite": data.json_blob.testrun.suite,
                    "revision": data.json_blob.test_build.revision,
                    "date": data.json_blob.testrun.date
                })
                Log.note("Added {{id}} from file", {"id": data.test_run_id})
            except Exception, e:
                Log.warning("Bad line ({{length}}bytes):\n\t{{prefix}}", {
                    "length": len(CNV.object2JSON(line)),
                    "prefix": CNV.object2JSON(line)[0:130]
                }, e)



def main():
    try:
        settings = startup.read_settings(filename="file2db_settings.json")
        Log.start(settings.debug)


        with DB(settings.db) as db:
            db.execute("""
                DROP TABLE IF EXISTS b2g_tests
            """)
            db.execute("""
                CREATE TABLE b2g_tests (
                    id INTEGER PRIMARY KEY NOT NULL,
                    branch VARCHAR(100),
                    name VARCHAR(100),
                    version VARCHAR(100),
                    suite varchar(200),
                    revision varchar(100),
                    `date` LONG
                )
            """)

            file2db(db, "b2g_tests", settings.source_file)
    except Exception, e:
        Log.error("can not seem to startup", e)


main()
