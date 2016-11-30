import json
import requests
import time

from elasticsearch import Elasticsearch
import csv

from config import config

'''
Provide updates in Slack every hour on new users and new sessions, which will help us monitor app performance
'''

def new(doc_type, size=1000, slackhook=config["slackhook"]):
    query = {
               "query": {
                  "match_all": {}
               }
            }
    results = es.search(body=query,
                        index="proprio",
                        doc_type=doc_type,
                        size=1000)

    count_all=0
    count_high_quality=0
    users = set()
    for i in results['hits']['hits']:
        minutes = str(int(round((int(i["_source"]["max_time"])-int(i["_source"]["session"]))*.001*.016667,0)))
        user = i["_source"]["userName"].encode("utf8")
        if (time.time()-int(i["_source"]["session"])/1000)/(60*60*24) < 31:
            if user not in users:
                if int(minutes) > 10:
                    count_high_quality += 1
                else:
                    count_all += 1
        users.add(user)
    data=json.dumps({"MAU -- *High Quality*": str(count_high_quality)})
    requests.post(url=slackhook, data=json.dumps({"text":data}), headers={"content-type": "text/javascript"})
    data=json.dumps({"MAU -- *All*": str(count_all)})
    requests.post(url=slackhook, data=json.dumps({"text":data}), headers={"content-type": "text/javascript"})

es_url = "localhost"
es_port = "9200"
es = Elasticsearch(es_url + ":" + es_port)
    
new("analysis_v2", 1000)
