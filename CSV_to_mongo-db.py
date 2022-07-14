from pymongo import MongoClient
import pandas as pd
import requests
import time
import pymongo
from pymongo import MongoClient
import json


def mongoimport(file_name, db_name, coll_name, db_url='localhost', db_port=27017):
    """ Imports a csv file at path csv_name to a mongo colection
    returns: count of the documants in the new collection
    """
    client = pymongo.MongoClient(db_url, db_port)
    db = client[db_name]
    coll = db[coll_name]
    data = pd.read_csv('{}.csv'.format(file_name),  encoding = 'ISO-8859-1')  
    data.to_json('{}.json'.format(file_name))
    jdf = open('{}.json'.format(file_name)).read()                        
    data = json.loads(jdf) 
    return coll.insert_one(data)


file_name ='netflix'  ##filename only without csv extension
db_name = 'database'
coll_name = 'netflix'
mongo =  mongoimport(file_name, db_name, coll_name, db_url='localhost', db_port=27017)














