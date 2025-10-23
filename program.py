import os
import pandas as pd
from pprint import pprint 
from pymongo import MongoClient, version

class DbConnector:
    """
    """

    def __init__(self,
                 DATABASE="movies",
                 HOST="localhost",
                 PORT=27017,
                 USER="root",
                 PASSWORD="secret123"):
        uri = f"mongodb://{USER}:{PASSWORD}@{HOST}:{PORT}/?authSource=admin"
        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

        print("You are connected to the database:", self.db.name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        self.client.close()
        print("\n-----------------------------------------------")
        print("Connection to %s-db is closed" % self.db.name)




class Program:

    
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    
    def setup(self, csv_folder = "."):
        db = self.client['movies']
        for filename in os.listdir(csv_folder):
            if filename.endswith(".csv"):
                collection_name = filename.replace(".csv", "")
                file_path = os.path.join(csv_folder, filename)

                try:
                    df = pd.read_csv(file_path)
                    df = df.where(pd.notnull(df), None)
                    data = df.to_dict("records")
                    if data:
                            db[collection_name].insert_many(data)
                except Exception as e:
                    print(f"ERROR: Failed to process {filename}: ", e)


    
    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)

   
def main():
    program = None
    try:
        program = Program()
        program.setup()
        program.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
