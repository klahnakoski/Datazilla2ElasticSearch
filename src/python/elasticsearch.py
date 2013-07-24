import sha
import requests
import time
from util.cnv import CNV
from util.debug import D
from util.basic import nvl
from util.map import Map, MapList

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
        time.sleep(2)
        return ElasticSearch(settings)




    @staticmethod
    def delete_index(settings, index=None):
        index=nvl(index, settings.index)

        ElasticSearch.delete(
            settings.host+":"+str(settings.port)+"/"+index,
        )

    #RETURN LIST OF {"alias":a, "index":i} PAIRS
    def get_aliases(self):
        response=requests.get(self.settings.host+":"+str(self.settings.port)+"/_aliases")
        data=CNV.JSON2object(response.content)
        output=[]
        for index, desc in data.items():
            if desc["aliases"] is None or len(desc["aliases"].items())==0:
                output.append({"index":index, "alias":None})
            else:
                for a, v in desc["aliases"].items():
                    output.append({"index":index, "alias":a})
        return MapList(output)


    #DELETE ALL INDEXES WITH GIVEN PREFIX, EXCEPT name
    def delete_all_but(self, prefix, name):
        for a in self.get_aliases():
            if a.index.startswith(prefix) and a.index!=name:
                ElasticSearch.delete_index(self.settings, a.index)


    def add_alias(self, alias):
        requests.post(
            self.settings.host+":"+str(self.settings.port)+"/_aliases",
            CNV.object2JSON({
                "actions":[
                    {"add":{"index":self.settings.index, "alias":alias}}
                ]
            })
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
            
            lines.extend('{"index":{"_id":"'+id+'"}}\n'+json+"\n")

        if len(lines)==0: return
        response=ElasticSearch.post(
            self.path+"/_bulk",
            data="".join(lines),
            headers={"Content-Type":"text"}
        )
        items=response["items"]

        for i, item in enumerate(items):
            if not item.index.ok:
                D.error(item.index.error+" while loading line:\n"+lines[i])

        if DEBUG: D.println("${num} items added", {"num":len(records)})


    # -1 FOR NO REFRESH
    def set_refresh_interval(self, seconds):
        if seconds<=0: interval="-1"
        else: interval=str(seconds)+"s"

        ElasticSearch.put(
             self.settings.host+":"+str(self.settings.port)+"/"+self.settings.index+"/_settings",
             data="{\"index.refresh_interval\":\""+interval+"\"}"
        )



    def search(self, query):
        try:
            return ElasticSearch.post(self.path+"/_search", data=CNV.object2JSON(query))
        except Exception, e:
            D.error("Problem with search", e)

    
        
    @staticmethod
    def post(*list, **args):
        try:
            response=requests.post(*list, **args)
            if DEBUG: D.println(response.content[:130])
            details=CNV.JSON2object(response.content)
            if details.error is not None:
                D.error(details.error)
            return details
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


