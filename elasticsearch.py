import sha
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


    def load(self, records, id_field=None):
        # ADD LINE WITH COMMAND
        lines=[]
        for r in records:
            json=CNV.object2JSON(r)

            if id_field is None:
                id=sha.new(json).hexdigest()
            else:
                id=str(r[id_field])
            
            lines.extend('{"create":{"_id":"'+id+'"}}\n'+json+"\n")

        if len(lines)==0: return
        response=ElasticSearch.post(
            self.path+"/_bulk",
            data="".join(lines),
            headers={"Content-Type":"text"}
        )
        if DEBUG: D.println("${num} items added", {"num":len(records)})


    # -1 FOR NO REFRESH
    def set_refresh_interval(self, seconds):
        ElasticSearch.put(
             self.settings.host+":"+str(self.settings.port)+"/"+self.settings.index+"/_settings",
             data="{\"index.refresh_interval\":\""+str(seconds)+"\"}"
        )



    def search(self, query):
        try:
            response=ElasticSearch.post(self.path+"/_search", data=CNV.object2JSON(query))
            if DEBUG: D.println(response.content[:100])
            return CNV.JSON2object(response.content)
        except Exception, e:
            D.error("Problem with search", e)

    
        
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


