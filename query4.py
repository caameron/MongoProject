import csv
import json
import sys, getopt, pprint
from datetime import datetime
from pymongo import MongoClient

mongo_client = MongoClient()

db=mongo_client.freeway

#station id for Foster NB = 1047

#create start and end date times
start1 = datetime(2011, 9, 22, 7, 0, 0)
end1 = datetime(2011, 9, 22, 9, 0, 0)

start2 = datetime(2011, 9, 22, 16, 0, 0)
end2 = datetime(2011, 9, 22, 18, 0, 0)

count1 = db.allLoopData.find({'stationid': "1047", 'starttime': {'$gte': start1, '$lt': end1}}).count()
print(count1)
count2 = db.allLoopData.find({'stationid': "1047", 'starttime': {'$gte': start2, '$lt': end2}}).count()
print(count2)
