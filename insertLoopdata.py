'''
Implementation based off of stack overflow post 
https://stackoverflow.com/questions/27416296/how-to-push-a-csv-data-to-mongodb-using-python/53431264
'''

import csv
import json
import sys, getopt, pprint
from datetime import datetime
from pymongo import MongoClient

csvfile = open('freeway_stations.csv', 'rU')
#csvfileLoop = open('freeway_loopdata_oneday.csv', 'rU')
csvfileLoop = open('freeway_loopdata.csv', 'rU')
reader = csv.DictReader(csvfile)
readerLoop = csv.DictReader(csvfileLoop)
mongo_client = MongoClient()

db=mongo_client.freeway

headers = ["stationid", "highwayid", "milepost", "locationtext", "upstream", "downstream", "stationclass", "numberlanes", "latlon", "length"]
headersLoop = ["detectorid", "starttime", "volume", "speed", "occupancy", "status", "dqflags"]

#Loop through loop data and add it to corresponding station array
station1045=[]
station1046=[]
station1047=[]
station1117=[]
station1048=[]
station1142=[]
station1140=[]
station1125=[]
station1098=[]
station1054=[]
station1053=[]
station1052=[]
station1051=[]
station1143=[]
station1141=[]
station1050=[]
station1049=[]
#db.loop.drop()

for row in readerLoop:
    entry={}

    #skip data that does not have speed
    if(row["speed"] == ""):
        continue;

    #go through loop data and put them in correct station
    for field in headersLoop:
        #Convert time
        if(field == "starttime"):
            #datetime_obj= datetime.strptime(row["starttime"], '%m/%d/%Y %H:%M:%S')
            time = row["starttime"]
            datetime_obj= datetime.strptime(time[:-3], '%Y-%m-%d %H:%M:%S')
            #datetime_obj= datetime.strptime(time, '%m/%d/%Y %H:%M:%S')
            entry[field] = datetime_obj
            continue;
        entry[field] = row[field]
    
    db.loopdata.insert(entry)
    
