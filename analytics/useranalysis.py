from elasticsearch import Elasticsearch
import pandas as pd
import time
import numpy as np
import requests

from config import config

es_url = "localhost"
es_port = "9200"
query = {"query": {"match_all":{}}}
results = es.search(body=query, index="proprio", doc_type="analysis_v2", size=1000)
sessions = pd.DataFrame(map(lambda x: x['_source'], results['hits']['hits']))
sessions = sessions.sort(['session'], ascending=[1])

# Overall Aggregate User Stats -- Exploring the Demographics of People Using our Product

x = set()
age_count = {}
height_count = {}
rating_count = {}
gender_count = {}
product_count = {}
for i in sessions.iterrows():
    user = str(i[1]["userName"].encode("utf-8"))
    age = str(i[1]["age"])
    height = str(i[1]["heightInches"])
    rating = str(i[1]["rating"])
    gender = str(i[1]["gender"])
    product = str(i[1]["manufacturer"] + "-" + i[1]["model"] + "-"+ i[1]["product"])

    if str(user) not in x:
        counter(age_count, age)
        counter(height_count, height)
        counter(rating_count, rating)
        counter(gender_count, gender)
        counter(product_count, product)
    x.add(user)

print(age_count)
print(height_count)
print(rating_count)
print(gender_count)
print(product_count)

# Create a .csv file with user stats for each session

with open("output.csv", 'w') as g:
g.write("user, minutes, hits, rallies, max_rally, mean_rally, date, product, hand, age, height, gender, rating" + "\n")
for i in sessions.iterrows():
    minutes = str(int(round((int(i[1]["max_time"])-int(i[1]["session"]))*.001*.016667,0)))
    user = i[1]["userName"].encode("utf-8")
    date = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(i[1]["session"])/1000)))
    hits = str(i[1]["aggregate"]["Forehands"] + i[1]["aggregate"]["Backhands"] + i[1]["aggregate"]["Serves"])
    rallies = str(len(i[1]['rallies']))
    hand = str(i[1]["hand"])
    product = i[1]["manufacturer"] + "-" + i[1]["model"] + "-"+ i[1]["product"]
    age = str(i[1]["age"])
    height = str(i[1]["heightInches"])
    rating = str(i[1]["rating"])
    gender = str(i[1]["gender"])
    max_rally = str(i[1]["max_rally"])
    mean_rally = str(i[1]["mean_rally"])
    g.write(user + ",")
    g.write(minutes + ",")
    g.write(hits + ",")
    g.write(rallies + ",")
    g.write(max_rally + ",")
    g.write(mean_rally + ",")
    g.write(date + ",")
    g.write(product + ",")
    g.write(hand + ",")
    g.write(age + ",")
    g.write(height + ",")
    g.write(gender + ",")
    g.write(rating + "\n")

    if int(minutes) > 10:
        if user != "Superman":
            if (time.time()-int(i[1]["session"])/1000)/(60*60*24) < 7:
                try:
                    print(str(user.encode("utf-8")) + " played " + str(minutes) + " minutes on " + str(date) + " with " + str(hits) + " strokes using a " + product + " and " + rallies + " rallies")
                except:
                    print("Exception")

g.close()



