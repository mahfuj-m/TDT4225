from pprint import pprint
from DbConnector import DbConnector
from pymongo import ASCENDING

import json


class Database:
    def __init__(self):
        self.connection=DbConnector()
        self.client=self.connection.client
        self.db=self.connection.db

    def create_coll(self, collection_name):
        coll=self.db.create_collection(collection_name)
        print('Created collection: ', coll)
    
    def create_coll_by_index(self, name):
        collection = self.db[name]  # Replace 'your_collection' with your desired collection name
        collection.create_index([('_id', ASCENDING)])

    def insert_documents(self,collection_name,documents):
        collection=self.db[collection_name]
        collection.insert_many(documents)

    
    def find_user(self,user_id):
        result = self.db['user'].find_one({"_id": user_id})
        return result

    def drop_coll(self,collection_name):
        coll=self.db[collection_name]
        coll.drop()
    
    def show_coll(self):
        collections = self.db.list_collection_names()
        # print(collections)
        return collections
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        with open('output.txt', 'w') as file:
            for doc in documents: 
                file.write(str(doc) + '\n')
        # 
        #     pprint(doc)
    def items(self,collection_name):
        return self.db[collection_name].count_documents({})

    def unique_items(self,collection_name,row):
        return self.db[collection_name].distinct(row)
    def get_activity(self,row):
        return self.db['activity'].find(row)
    
