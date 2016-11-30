from elasticsearch import Elasticsearch
from config import config
import operator

es = Elasticsearch(config["es_url"] + ":" + config["es_port"])

query = {"query": { "match_all": {} } }

results = es.search(body=query,
                    index=config["es_index"],
                    doc_type=config["es_analyzed_doc"],
                    size=100000)

hits = {}
for i in results["hits"]["hits"]:
    if str((i["_source"]["userId"])) not in hits.keys():
        hits[str((i["_source"]["userId"]))] = i["_source"]["aggregate"]["Backhands"] + i["_source"]["aggregate"]["Forehands"] + i["_source"]["aggregate"]["Serves"]
    else:
        hits[str((i["_source"]["userId"]))] += i["_source"]["aggregate"]["Backhands"] + i["_source"]["aggregate"]["Forehands"] + i["_source"]["aggregate"]["Serves"]
sorted_hits = sorted(hits.items(), key=operator.itemgetter(1), reverse=True)

maxrally = {}
for i in results["hits"]["hits"]:
    if str((i["_source"]["userId"])) not in maxrally.keys():
        maxrally[str((i["_source"]["userId"]))] = i["_source"]["max_rally"]
    elif i["_source"]["max_rally"] > maxrally[str((i["_source"]["userId"]))]:
        maxrally[str((i["_source"]["userId"]))] = i["_source"]["max_rally"]

sorted_maxrally = sorted(maxrally.items(), key=operator.itemgetter(1), reverse=True)

meanrally = {}
for i in results["hits"]["hits"]:
    for j in i["_source"]["rallies"]:
        if str((i["_source"]["userId"])) not in meanrally.keys():
            meanrally[str((i["_source"]["userId"]))] = [len(j["rally"]), 1, len(j["rally"])]
        else:
            meanrally[str((i["_source"]["userId"]))][0] += len(j["rally"])
            meanrally[str((i["_source"]["userId"]))][1] += 1
            meanrally[str((i["_source"]["userId"]))][2] = float(meanrally[str((i["_source"]["userId"]))][0])/float(meanrally[str((i["_source"]["userId"]))][1])

sorted_meanrally = sorted(meanrally.items(), key=operator.itemgetter(1), reverse=True)

data = {}
for index, i in enumerate(sorted_hits):
    data[i[0]] = {"hits": index + 1}

for index, i in enumerate(sorted_maxrally):
    data[i[0]]["max_rally"] = index + 1

for index, i in enumerate(sorted_meanrally):
    data[i[0]]["mean_rally"] = index + 1

es.indices.delete(index='rankings', ignore=[400, 404])
for k, v in data.items():
    try:
        body = {"userId": k, "meanrally": v["mean_rally"], "maxrally": v["max_rally"], "hits": v["hits"]}
    except KeyError:
        body = {"userId": k, "meanrally": 0, "maxrally": 0, "hits": v["hits"]}
    print(body)
    res = es.index(index="rankings", doc_type='ranks', body=body)
