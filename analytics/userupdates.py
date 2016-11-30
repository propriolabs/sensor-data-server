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

    old=[]
    new=[]
    new_high_quality=[]

    with open('data/' + doc_type + '.csv') as csvfile:
        reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in reader:
            old.append(row[0])

    for i in results['hits']['hits']:
        if doc_type=="users":
            item = str(i["_source"]["email"])
            if item not in old: 
                new.append(item)
                output = open('data/' + doc_type + '.csv','a')
                output.write(item + "\n")
                output.close()
        elif doc_type=="analysis_v2":
            item = str(i["_id"])
            if item not in old: 
                minutes = str(int(round((int(i["_source"]["max_time"])-int(i["_source"]["session"]))*.001*.016667,0)))
                user = i["_source"]["userName"].encode("utf8")
                date = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(i["_source"]["session"])/1000)))
                hits = str(i["_source"]["aggregate"]["Forehands"] + i["_source"]["aggregate"]["Backhands"] + i["_source"]["aggregate"]["Serves"])
                rallies = str(len(i["_source"]['rallies']))
                hand = str(i["_source"]["hand"])
                product = i["_source"]["manufacturer"] + "-" + i["_source"]["model"] + "-"+ i["_source"]["product"]
                age = str(i["_source"]["age"])
                height = str(i["_source"]["heightInches"])
                rating = str(i["_source"]["rating"])
                gender = str(i["_source"]["gender"])
                max_rally = str(i["_source"]["max_rally"])
                mean_rally = str(i["_source"]["mean_rally"])
                if int(minutes) > 10:
                    try:
                        new_high_quality.append((str(user) + " played " + str(minutes) + " minutes on " + str(date) + " with " + str(hits) + " strokes using a " + product + " and " + rallies + " rallies"))
                    except:
                        new_high_quality.append(("unicode_error played " + str(minutes) + " minutes on " + str(date) + " with " + str(hits) + " strokes using a " + product + " and " + rallies + " rallies"))
                else:
                    try:
                        new.append(str(user) + " played " + str(minutes) + " minutes on " + str(date) + " with " + str(hits) + " strokes using a " + product + " and " + rallies + " rallies")
                    except:
                        new.append(("unicode_error played " + str(minutes) + " minutes on " + str(date) + " with " + str(hits) + " strokes using a " + product + " and " + rallies + " rallies"))
                output = open('data/' + doc_type + '.csv','a')
                output.write(item + "\n")
                output.close()
    if doc_type=="users":
        if len(new)>0:
            data=json.dumps({"new users": str(", ".join(new))})
            requests.post(url=slackhook, data=json.dumps({"text":data}), headers={"content-type": "text/javascript"})
        else:
            print("No new " + doc_type)
    elif doc_type=="analysis_v2":
        if len(new)>0:
            data=json.dumps({"new sessions *Short Session*": str(", ".join(new))})
            requests.post(url=slackhook, data=json.dumps({"text":data}), headers={"content-type": "text/javascript"})
            data=json.dumps({"new sessions *Real Session*": str(", ".join(new_high_quality))})
            requests.post(url=slackhook, data=json.dumps({"text":data}), headers={"content-type": "text/javascript"})
        else:
            print("No new sessions")

es_url = "localhost"
es_port = "9200"
es = Elasticsearch(es_url + ":" + es_port)
    

new("users", 1000)
new("analysis_v2", 1000)
