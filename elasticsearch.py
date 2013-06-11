import requests
from util.map import Map
from util.cnv import CNV
from util.debug import D


class ElasticSearch():

    def __init__(self, **param):
        param=Map(**param)
        assert param.host is not None
        assert param.port is not None
        assert param.index_name is not None
        assert param.type_name is not None

        self.settings=param
        self.path=self.settings.host+":"+str(self.settings.port)+"/"+self.settings.index_name+"/"+self.settings.type_name





    def load(self, records):
        # ADD LINE WITH COMMAND
        ['{"create":{"id":"'+str(r.id)+'"}}\n'+CNV.object2JSON(r) for r in records]
        response=requests.post(self.path+"_bulk", "\n".join([CNV.object2JSON(r) for r in records]))
        D.println(response.content)
        