
import requests
from util.cnv import CNV
from util.debug import D

DEBUG=True

class ElasticSearch():




    def __init__(self, settings):
        assert settings.host is not None
        assert settings.index is not None
        assert settings.type is not None

        if settings.port is None: settings.port=9200
        
        self.settings=settings
        self.path=settings.host+":"+str(settings.port)+"/"+settings.index+"/"+settings.type



    @staticmethod
    def create_index(settings, schema):
        ElasticSearch.post(
            settings.host+":"+str(settings.port)+"/"+settings.index,
            data=CNV.object2JSON(schema),
            headers={"Content-Type":"application/json"}
        )


    @staticmethod
    def delete_index(settings):
        ElasticSearch.delete(
            settings.host+":"+str(settings.port)+"/"+settings.index,
        )


    def load(self, records):
        # ADD LINE WITH COMMAND
        lines=['{"create":{"_id":"'+str(r.id)+'"}}\n'+CNV.object2JSON(r)+"\n" for r in records]
        response=ElasticSearch.post(
            self.path+"/_bulk",
            data="".join(lines),
            headers={"Content-Type":"text"}
        )
        if DEBUG: D.println("${num} items added", {"num":len(CNV.JSON2object(response.content).items)})


    # -1 FOR NO REFRESH
    def set_refresh_interval(self, seconds):
        ElasticSearch.put(self.path+"/_settings", data="{\"index\":{\"refresh_interval\":\""+str(seconds)+"\"}}")


        
    @staticmethod
    def post(*list, **args):
        try:
            response=requests.post(*list, **args)
            if DEBUG: D.println(response.content[:100])
            return response
        except Exception, e:
            D.error("Problem with call to ${url}", {"url":list[0]}, e)

    @staticmethod
    def put(*list, **args):
        try:
            response=requests.put(*list, **args)
            if DEBUG: D.println(response.content)
            return response
        except Exception, e:
            D.error("Problem with call to ${url}", {"url":list[0]}, e)

    @staticmethod
    def delete(*list, **args):
        try:
            response=requests.delete(*list, **args)
            if DEBUG: D.println(response.content)
            return response
        except Exception, e:
            D.error("Problem with call to ${url}", {"url":list[0]}, e)


