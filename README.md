Datazilla2ElasticSearch
=======================

Copy objectstore to ES for faster charting




Installation
------------

Install Pandas:

    http://pandas.pydata.org/getpandas.html

Clone from github:

    git clone https://github.com/klahnakoski/Datazilla2ElasticSearch.git

Pull other python dependencies:

    pip install -r requirements.txt


Settings
--------

You will require a ```settings.json``` file that holds all your pointers

	{
		"pushlog":{
			"host":"s4n4.qa.phx1.mozilla.com",
			"port":3306,
			"username":"username",
			"password":"password",
			"schema":"pushlog_hgmozilla_1"
		},
		"elasticsearch":{
			"host":"http://klahnakoski-es.corp.tor1.mozilla.com",
			"port":9200,
			"index":"datazilla",
			"type":"test_results"
		},
		"production":{
			"blob_url":"https://datazilla.mozilla.org/talos/refdata/objectstore/json_blob",
			"threads":20,
			"min": 1490000,
			"max": 3000000
		},
		"param":{
		    "output_file":"data/raw_json_blobs.tab"
		}
	}



