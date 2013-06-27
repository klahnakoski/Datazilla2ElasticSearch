from math import floor, ceil
from util.cnv import CNV
from util.debug import D
from util.stats import Z_moment
from util.strings import right


DEBUG=False




def transform(r, datazilla_id, keep_arrays_smaller_than=25):
    #CONVERT AGE TIMINGS TO ARRAY OF PAGE TIMINGS
    r.datazilla_id=datazilla_id
    
    r.results=[{
        "name":k,
        "moments": Z_moment.new_instance(v).dict,
        "samples": (dict(("s"+right("00"+str(i), 2), s) for i, s in enumerate(v)) if len(v)<=keep_arrays_smaller_than else None)
    } for k,v in r.results.items()]

    #CONVERT UNIX TIMESTAMP TO MILLISECOND TIMESTAMP
    r.testrun.date*=1000

    if r.results_aux is not None:
        r.results_aux.responsivness     ={"moments":Z_moment.new_instance(r.results_aux.responsivness   ).dict}
        r.results_aux["Private bytes"]  ={"moments":Z_moment.new_instance(r.results_aux["Private bytes"]).dict}
        r.results_aux.Main_RSS          ={"moments":Z_moment.new_instance(r.results_aux.Main_RSS        ).dict}
        r.results_aux.shutdown          ={"moments":Z_moment.new_instance(r.results_aux.shutdown        ).dict}

    if r.results_xperf is not None:
        r.results_xperf.mainthread_writebytes=[{"path":i[1], "value":i[0]} for i in r.results_xperf.mainthread_writebytes]
        r.results_xperf.mainthread_readcount=[{"path":i[1], "value":i[0]} for i in r.results_xperf.mainthread_readcount]
        r.results_xperf.mainthread_writecount=[{"path":i[1], "value":i[0]} for i in r.results_xperf.mainthread_writecount]
        r.results_xperf.mainthread_readbytes=[{"path":i[1], "value":i[0]} for i in r.results_xperf.mainthread_readbytes]
#    summarize(r.dict, keep_arrays_smaller_than)
    return r






# RESPONSIBLE FOR CONVERTING LONG ARRAYS OF NUMBERS TO THEIR REPRESENTATIVE
# MOMENTS
def summarize(r, keep_arrays_smaller_than=25):

    try:
        if isinstance(r, dict):
            for k, v in [(k,v) for k,v in r.items()]:
                new_v=summarize(v)
                if isinstance(new_v, Z_moment):
                    #CONVERT MOMENTS' TUPLE TO NAMED HASH (FOR EASIER ES INDEXING)
                    new_v={"moment":new_v.dict}


                    if isinstance(v, list) and len(v)<=keep_arrays_smaller_than:
                        #KEEP THE SMALL SAMPLES
                        new_v["samples"]=dict([("x"+("0"+str(i))[-2:],v) for i, v in enumerate(v)])

                r[k]=new_v
        elif isinstance(r, list):
            try:
                bottom=0.0
                top=0.9

                ## keep_middle - THE PROPORTION [0..1] OF VALUES TO KEEP, EXTREMES ARE REJECTED
                values=[float(v) for v in r]

                length=len(values)
                min=int(floor(length*bottom))
                max=int(ceil(length*top))
                values=sorted(values)[min:max]
                if DEBUG: D.println("${num} of ${total} used in moment", {"num":len(values), "total":length})

                return Z_moment.new_instance(values)
            except Exception, e:
                for i, v in enumerate(r):
                    r[i]=summarize(v)
        return r
    except Exception, e:
        D.warning("Can not summarize: ${json}", {"json":CNV.object2JSON(r)})




#HANDLE results_xperf
#{
#    "test_machine" : {
#        "platform"  : "x86",
#        "osversion" : "6.1.7601",
#        "os"        : "win",
#        "name"      : "t-w732-ix-047"
#    },
#    "results_xperf" : {
#        "nonmain_normal_netio" : [
#            156175972.0
#        ],
#        "mainthread_writebytes" : [
#            [
#                32,
#                "C:\\ProgramData\\NVIDIA Corporation\\Drs\\nvAppTimestamps"
#            ],
#            [
#                686,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\compatibility.ini"
#            ]
#        ],
#        "main_startup_fileio" : [
#            34340565.0
#        ],
#        "main_normal_netio" : [
#            7602201.0
#        ],
#        "nonmain_startup_fileio" : [
#            489646.0
#        ],
#        "main_startup_netio" : [
#            5.0
#        ],
#        "nonmain_normal_fileio" : [
#            485952041.0
#        ],
#        "main_normal_fileio" : [
#            188517284.0
#        ],
#        "mainthread_readcount" : [
#            [
#                4,
#                "C:\\slave\\talos-data\\firefox\\mozglue.dll"
#            ],
#            [
#                6,
#                "C:\\Users\\cltbld\\Desktop\\desktop.ini"
#            ],
#            [
#                2,
#                "C:\\windows\\Prefetch\\FIREFOX.EXE-E5F22DED.pf"
#            ],
#            [
#                2,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\pluginreg.dat"
#            ],
#            [
#                4,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\prefs.js"
#            ],
#            [
#                2,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\mimeTypes.rdf"
#            ],
#            [
#                8,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\places.sqlite"
#            ],
#            [
#                2,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\extensions\\pageloader@mozilla.org\\chrome.manifest"
#            ],
#            [
#                2,
#                "C:\\Program Files\\desktop.ini"
#            ],
#            [
#                2,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\places.sqlite-shm"
#            ],
#            [
#                26,
#                "C:\\slave\\talos-data\\firefox\\browser\\omni.ja"
#            ],
#            [
#                54,
#                "C:\\slave\\talos-data\\firefox\\nss3.dll"
#            ],
#            [
#                2,
#                "C:\\slave\\talos-data\\firefox\\browser\\chrome.manifest"
#            ],
#            [
#                2,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\localstore.rdf"
#            ],
#            [
#                34,
#                "C:\\slave\\talos-data\\firefox\\browser\\blocklist.xml"
#            ],
#            [
#                2,
#                "C:\\slave\\talos-data\\firefox\\mozalloc.dll"
#            ],
#            [
#                2,
#                "C:\\windows\\Fonts\\StaticCache.dat"
#            ],
#            [
#                102,
#                "C:\\slave\\talos-data\\firefox\\mozjs.dll"
#            ],
#            [
#                2,
#                "C:\\Users\\cltbld\\AppData\\Roaming\\Mozilla\\firefox\\profiles.ini"
#            ],
#            [
#                14,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\permissions.sqlite"
#            ],
#            [
#                24,
#                "C:\\ProgramData\\NVIDIA Corporation\\Drs\\nvAppTimestamps"
#            ],
#            [
#                2,
#                "C:\\slave\\talos-data\\firefox\\browser\\components\\components.manifest"
#            ],
#            [
#                4,
#                "C:\\slave\\talos-data\\firefox\\dependentlibs.list"
#            ],
#            [
#                2,
#                "C:\\windows\\system32\\spool\\drivers\\color\\sRGB Color Space Profile.icm"
#            ],
#            [
#                2,
#                "C:\\ProgramData\\NVIDIA Corporation\\Drs\\nvdrssel.bin"
#            ],
#            [
#                2,
#                "C:\\Users\\cltbld\\AppData\\Roaming\\Mozilla\\firefox\\Crash Reports\\InstallTime20130622210658"
#            ],
#            [
#                2,
#                "C:\\Users\\cltbld\\AppData\\Roaming\\Mozilla\\firefox\\Crash Reports\\LastCrash"
#            ],
#            [
#                6,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\content-prefs.sqlite"
#            ],
#            [
#                46,
#                "C:\\slave\\talos-data\\firefox\\omni.ja"
#            ],
#            [
#                24,
#                "C:\\slave\\talos-data\\firefox\\msvcr100.dll"
#            ],
#            [
#                4,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\user.js"
#            ],
#            [
#                4,
#                "C:\\slave\\talos-data\\firefox\\defaults\\pref\\channel-prefs.js"
#            ],
#            [
#                14,
#                "C:\\slave\\talos-data\\firefox\\msvcp100.dll"
#            ],
#            [
#                2,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\extensions.ini"
#            ],
#            [
#                8,
#                "C:\\slave\\talos-data\\talos\\page_load_test\\tp5n\\tp5n.manifest"
#            ],
#            [
#                2,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\compatibility.ini"
#            ],
#            [
#                12,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\search.json"
#            ],
#            [
#                666,
#                "C:\\slave\\talos-data\\firefox\\xul.dll"
#            ],
#            [
#                100,
#                "C:\\slave\\talos-data\\firefox\\gkmedias.dll"
#            ],
#            [
#                2,
#                "C:\\Users\\desktop.ini"
#            ]
#        ],
#        "mainthread_writecount" : [
#            [
#                4,
#                "C:\\ProgramData\\NVIDIA Corporation\\Drs\\nvAppTimestamps"
#            ],
#            [
#                34,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\compatibility.ini"
#            ]
#        ],
#        "mainthread_readbytes" : [
#            [
#                262144,
#                "C:\\slave\\talos-data\\firefox\\mozglue.dll"
#            ],
#            [
#                1692,
#                "C:\\Users\\cltbld\\Desktop\\desktop.ini"
#            ],
#            [
#                6540,
#                "C:\\windows\\Prefetch\\FIREFOX.EXE-E5F22DED.pf"
#            ],
#            [
#                1892,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\pluginreg.dat"
#            ],
#            [
#                25200,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\prefs.js"
#            ],
#            [
#                8192,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\mimeTypes.rdf"
#            ],
#            [
#                196808,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\places.sqlite"
#            ],
#            [
#                600,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\extensions\\pageloader@mozilla.org\\chrome.manifest"
#            ],
#            [
#                352,
#                "C:\\Program Files\\desktop.ini"
#            ],
#            [
#                65536,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\places.sqlite-shm"
#            ],
#            [
#                1703936,
#                "C:\\slave\\talos-data\\firefox\\browser\\omni.ja"
#            ],
#            [
#                3538944,
#                "C:\\slave\\talos-data\\firefox\\nss3.dll"
#            ],
#            [
#                80,
#                "C:\\slave\\talos-data\\firefox\\browser\\chrome.manifest"
#            ],
#            [
#                8192,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\localstore.rdf"
#            ],
#            [
#                139264,
#                "C:\\slave\\talos-data\\firefox\\browser\\blocklist.xml"
#            ],
#            [
#                131072,
#                "C:\\slave\\talos-data\\firefox\\mozalloc.dll"
#            ],
#            [
#                120,
#                "C:\\windows\\Fonts\\StaticCache.dat"
#            ],
#            [
#                6684672,
#                "C:\\slave\\talos-data\\firefox\\mozjs.dll"
#            ],
#            [
#                8192,
#                "C:\\Users\\cltbld\\AppData\\Roaming\\Mozilla\\firefox\\profiles.ini"
#            ],
#            [
#                4424,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\permissions.sqlite"
#            ],
#            [
#                908,
#                "C:\\ProgramData\\NVIDIA Corporation\\Drs\\nvAppTimestamps"
#            ],
#            [
#                68,
#                "C:\\slave\\talos-data\\firefox\\browser\\components\\components.manifest"
#            ],
#            [
#                16384,
#                "C:\\slave\\talos-data\\firefox\\dependentlibs.list"
#            ],
#            [
#                8192,
#                "C:\\windows\\system32\\spool\\drivers\\color\\sRGB Color Space Profile.icm"
#            ],
#            [
#                2,
#                "C:\\ProgramData\\NVIDIA Corporation\\Drs\\nvdrssel.bin"
#            ],
#            [
#                20,
#                "C:\\Users\\cltbld\\AppData\\Roaming\\Mozilla\\firefox\\Crash Reports\\InstallTime20130622210658"
#            ],
#            [
#                20,
#                "C:\\Users\\cltbld\\AppData\\Roaming\\Mozilla\\firefox\\Crash Reports\\LastCrash"
#            ],
#            [
#                65768,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\content-prefs.sqlite"
#            ],
#            [
#                3014656,
#                "C:\\slave\\talos-data\\firefox\\omni.ja"
#            ],
#            [
#                1572864,
#                "C:\\slave\\talos-data\\firefox\\msvcr100.dll"
#            ],
#            [
#                11408,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\user.js"
#            ],
#            [
#                1432,
#                "C:\\slave\\talos-data\\firefox\\defaults\\pref\\channel-prefs.js"
#            ],
#            [
#                917504,
#                "C:\\slave\\talos-data\\firefox\\msvcp100.dll"
#            ],
#            [
#                8192,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\extensions.ini"
#            ],
#            [
#                32768,
#                "C:\\slave\\talos-data\\talos\\page_load_test\\tp5n\\tp5n.manifest"
#            ],
#            [
#                8192,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\compatibility.ini"
#            ],
#            [
#                33254,
#                "C:\\Users\\cltbld\\AppData\\Local\\Temp\\tmpgg7eoh\\profile\\search.json"
#            ],
#            [
#                43646976,
#                "C:\\slave\\talos-data\\firefox\\xul.dll"
#            ],
#            [
#                6553600,
#                "C:\\slave\\talos-data\\firefox\\gkmedias.dll"
#            ],
#            [
#                352,
#                "C:\\Users\\desktop.ini"
#            ]
#        ]
#    },
#    "testrun" : {
#        "date"    : 1371972423000,
#        "suite"   : "tp5n",
#        "options" : {
#            "responsiveness"  : false,
#            "tpmozafterpaint" : true,
#            "tpchrome"        : true,
#            "tppagecycles"    : 1,
#            "tpcycles"        : 1,
#            "tprender"        : false,
#            "shutdown"        : false,
#            "extensions" : [
#                {
#                    "name" : "pageloader@mozilla.org"
#                }
#            ],
#            "rss"             : true
#        }
#    },
#    "results" : [ ],
#    "test_build" : {
#        "version"  : "24.0a1",
#        "revision" : "6b2f29bc6da8",
#        "name"     : "Firefox",
#        "branch"   : "Mozilla-Inbound",
#        "id"       : "20130622210658"
#    }
#}
