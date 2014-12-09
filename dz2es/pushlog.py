import requests
from dz2es.mozilla_graph import MozillaGraph
from dz2es.repos.revisions import Revision
from pyLibrary import convert
from pyLibrary.env.logs import Log
from pyLibrary.structs import Struct


class Pushlog(object):


    def __init__(self):
        repos = convert.JSON2object(convert.utf82unicode(requests.get("https://treeherder.mozilla.org/api/repository/").content))
        self.branches = {b.name.lower(): b for b in repos}
        self.graph = MozillaGraph(Struct(branches=self.branches))


    def __getitem__(self, item):
        try:
            if not isinstance(item, (tuple, list)):
                Log.error("Expecting a [branch, rev] pair")

            push = self.graph.get_push(Revision(
                branch=self.branches[item[0].lower()],
                changeset={"id": item[1]}
            ))
        except Exception, e:
            return None

        return push.date

    def keys(self):
        return self.branches.keys()



