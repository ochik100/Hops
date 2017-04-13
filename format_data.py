import numpy as np
import pandas as pd
from pymongo import MongoClient


def connect_to_database(database_name):
    '''
    Connect to MongoDB (must have Mongo server running to connect)
    INPUT:
        database_name (st)
    OUTPUT:
        db (MongoDB)
    '''
    client = MongoClient()
    db = client[database_name]
    if db:
        print "Connected to", database_name
    return db


def convert_collection_to_df(db, collection_name):
    '''
    Convert collection in MongoDB to pandas dataframe
    INPUT:
        db (MongoDB): mongo database instance
        collection_name (str)
    OUTPUT:
        df (DataFrame)
    '''
    input_data = db[collection_name]
    df = pd.DataFrame(list(input_data.find()))
    print "Converted", collection_name, "to dataframe"
    return df

if __name__ == '__main__':
    db = connect_to_database('beer_advocates')
    df = convert_collection_to_df(db, 'reviews')
