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

import requests
from dz2es.mozilla_graph import MozillaGraph
from dz2es.repos.revisions import Revision
from pyLibrary import convert
from pyLibrary.env.logs import Log
from pyLibrary.structs import Struct


class Pushlog(object):


    def __init__(self):
        repos = convert.JSON2object(convert.utf82unicode(requests.get("https://treeherder.mozilla.org/api/repository/").content))

        self.branches = {talos2treeherder(b.name): b for b in repos}
        self.graph = MozillaGraph(Struct(branches=self.branches))


    def __getitem__(self, item):
        try:
            if not isinstance(item, (tuple, list)):
                Log.error("Expecting a [branch, rev] pair")

            if item[1] == "NULL":
                return None

            push = self.graph.get_push(Revision(
                branch=self.branches[talos2treeherder(item[0])],
                changeset={"id": item[1]}
            ))
        except Exception, e:
            return None

        return push.date

    def keys(self):
        return self.branches.keys()



def talos2treeherder(name):
    name = name.lower()
    name = name.replace(".", "_")

    if name.endswith("-non-pgo"):
        name = name[:-8]
    if name == "mozilla-central":
        return "firefox"
    else:
        return name