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
csvfileLoop = open('freeway_loopdata_oneday.csv', 'rU')
#csvfileLoop = open('freeway_loopdatasmall.csv', 'rU')
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
#db.stationtest.drop()

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
            #datetime_obj= datetime.strptime(time[:-3], '%Y-%m-%d %H:%M:%S')
            datetime_obj= datetime.strptime(time, '%m/%d/%Y %H:%M:%S')
            entry[field] = datetime_obj
            continue;
        entry[field] = row[field]
    

    det = row["detectorid"]
    if(det == '1345' or det == '1346' or det == '1347' or det == '1348'):
        station1045.append(entry)
    elif(det == '1353' or det == '1354' or det == '1355'):
        station1046.append(entry)
    elif(det == '1361' or det == '1362' or det == '1363'):
        station1047.append(entry)
    elif(det == '1809' or det == '1810' or det == '1811'):
        station1117.append(entry)
    elif(det == '1369' or det == '1370' or det == '1371'):
        station1048.append(entry)
    elif(det == '1949' or det == '1950' or det == '1951'):
        station1142.append(entry)
    elif(det == '1941' or det == '1942' or det == '1943'):
        station1140.append(entry)
    elif(det == '1856' or det == '1857' or det == '1858'):
        station1125.append(entry)
    elif(det == '1730' or det == '1731' or det == '1732'):
        station1098.append(entry)
    elif(det == '1417' or det == '1418' or det == '1419'):
        station1054.append(entry)
    elif(det == '1409' or det == '1410' or det == '1411'):
        station1053.append(entry)
    elif(det == '1401' or det == '1402' or det == '1403'):
        station1052.append(entry)
    elif(det == '1393' or det == '1394' or det == '1395'):
        station1051.append(entry)
    elif(det == '1953' or det == '1954' or det == '1955'):
        station1143.append(entry)
    elif(det == '1945' or det == '1946' or det == '1947'):
        station1141.append(entry)
    elif(det == '1385' or det == '1386' or det == '1387'):
        station1050.append(entry)
    elif(det == '1377' or det == '1378' or det == '1379'):
        station1049.append(entry)
        

for row in reader:
    entry={}
    #Addin station data
    for field in headers:
        entry[field] = row[field]

    #add loop data to staion
    station = row["stationid"]
    if(station == "1045"):
        entry["loop_data"] = station1045
    elif(station == "1046"):
        entry["loop_data"] = station1046
    elif(station == "1047"):
        entry["loop_data"] = station1047
    elif(station == "1117"):
        entry["loop_data"] = station1117
    elif(station == "1048"):
        entry["loop_data"] = station1048
    elif(station == "1142"):
        entry["loop_data"] = station1142
    elif(station == "1140"):
        entry["loop_data"] = station1140
    elif(station == "1125"):
        entry["loop_data"] = station1125
    elif(station == "1098"):
        entry["loop_data"] = station1098
    elif(station == "1054"):
        entry["loop_data"] = station1054
    elif(station == "1053"):
        entry["loop_data"] = station1053
    elif(station == "1052"):
        entry["loop_data"] = station1052
    elif(station == "1051"):
        entry["loop_data"] = station1051
    elif(station == "1143"):
        entry["loop_data"] = station1143
    elif(station == "1141"):
        entry["loop_data"] = station1141
    elif(station == "1050"):
        entry["loop_data"] = station1050
    elif(station == "1049"):
        entry["loop_data"] = station1049
    else:
        print("NO MATCH")


    db.stations.insert(entry)

