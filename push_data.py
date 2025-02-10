import os
import json
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MONGO_DB_URL = os.getenv("MONGO_DB_URL")

if not MONGO_DB_URL:
    raise ValueError("MONGO_DB_URL is not set. Please check your .env file!")

import certifi  # Provides root certificates for secure HTTP connections
import pandas as pd
import pymongo
from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import logging


class NetworkDataExtract:
    def __init__(self):
        try:
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL, tlsCAFile=certifi.where())
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def csv_to_json_converter(self, file_path):
        """Convert CSV data to JSON format for MongoDB insertion."""
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = json.loads(data.to_json(orient="records")) 
            return records
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def insert_data_to_mongodb(self, records, database, collection):
        """Insert JSON data into MongoDB."""
        try:
            db = self.mongo_client[database]
            coll = db[collection]
            coll.insert_many(records)
            return len(records)  # Return number of inserted records
        except Exception as e:
            raise NetworkSecurityException(e, sys)


if __name__ == '__main__':
    FILE_PATH = "network_data/phisingData.csv"
    DATABASE = "GARV"
    COLLECTION = "Network_data"

    network_obj = NetworkDataExtract()

    # Convert CSV to JSON
    records = network_obj.csv_to_json_converter(file_path=FILE_PATH)
    print(f"Converted {len(records)} records from CSV.")

    # Insert data into MongoDB
    num_inserted = network_obj.insert_data_to_mongodb(records=records, database=DATABASE, collection=COLLECTION)
    print(f"Inserted {num_inserted} records into MongoDB.")
