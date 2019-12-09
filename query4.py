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

#Aggregations to sum all speeds and average it out
sumSpeeds1 = [ {'$match': {'stationid':1047, 'starttime': {'$gte': start1, '$lt': end1}}}, {'$group': {'_id': 1, 'totalSpeed': {'$sum': '$speed'}, 'count': {'$sum': 1}, 'avg': {'$avg': '$speed'}}} ]
sumSpeeds2 = [ {'$match': {'stationid':1047, 'starttime': {'$gte': start2, '$lt': end2}}}, {'$group': {'_id': 1, 'totalSpeed': {'$sum': '$speed'}, 'count': {'$sum': 1}, 'avg': {'$avg': '$speed'}}} ]

#Query Database to get total speed, count, and average speed for those hours
count1 = list(db.loopdata.aggregate(sumSpeeds1))
count2 = list(db.loopdata.aggregate(sumSpeeds2))

#Print out values
totalSpeed = count1[0]['totalSpeed'] + count2[0]['totalSpeed']
totalCount = count1[0]['count'] + count2[0]['count']
avgSpeed = (count1[0]['avg'] + count2[0]['avg'])/2
print "Total Speed:", totalSpeed
print "Total Count:", totalCount
print "Average Speed:", avgSpeed

#Calculate and print out average travel time
travelTime = 1.6/avgSpeed * 3660
print "Average Travel Time:", travelTime
