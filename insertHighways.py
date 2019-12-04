'''
Implementation based off of stack overflow post 
https://stackoverflow.com/questions/27416296/how-to-push-a-csv-data-to-mongodb-using-python/53431264
'''

import csv
import json
import sys, getopt, pprint
from pymongo import MongoClient

csvfile = open('highways.csv', 'rU')
reader = csv.DictReader(csvfile)
mongo_client = MongoClient()

db=mongo_client.freeway

headers = ["highwayid", "shortdirection", "direction", "highwayname"]

for row in reader:
    entry={}
    for field in headers:
        entry[field] = row[field]

    db.highways.insert(entry)
