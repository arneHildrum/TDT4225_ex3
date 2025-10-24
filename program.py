import os
import ast
import pandas as pd
from pprint import pprint 
from pymongo import MongoClient, version


def run(program):
    task = 100
    while(task != 0):
        task = int(input("Enter task number (0-10): "))
        if task == 0:
            print("Shutting down program.......")
        else:
            match task:
                case 1: program.task_1()
                case 2: program.task_2()
                case 3: program.task_3()
                case 4: program.task_4()
                case 5: program.task_5()
                case 6: program.task_6()
                case 7: program.task_7()
                case 8: program.task_8()
                case 9: program.task_9()
                case 10: program.task_10()

def main():
    program = None
    try:
        program = Program()
        #program.setup()
        program.show_coll()
        run(program)
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


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
                    for col in df.columns:
                        if df[col].astype(str).str.startswith("[").any():
                            try:
                                df[col] = df[col].apply(
                                    lambda x: ast.literal_eval(x)
                                    if isinstance(x, str) and x.strip().startswith("[")
                                    else x
                                )
                            except Exception as e:
                                print(f"Skipping parsing of column {col} due to error: {e}")
                    data = df.to_dict("records")
                    if data:
                            db[collection_name].insert_many(data)
                except Exception as e:
                    print(f"ERROR: Failed to process {filename}: ", e)

    
    def show_coll(self):
        collections = self.client['movies'].list_collection_names()
        print(collections)


    def task_1(self):
        pipeline = [
            {"$unwind": "$crew"},
            {"$match": {"crew.job": "Director"}},
            {"$group": {
                "_id": "$crew.name",
                "movie_count": {"$sum": 1},
                "revenues": {"$push": "$revenue"},
                "avg_vote": {"$avg": "$vote_average"}
            }},
            {"$match": {"movie_count": {"$gte": 5}}},
            {"$project": {
                "movie_count": 1,
                "avg_vote": 1,
                "median_revenue": {
                    "$let": {
                        "vars": {"sorted": {"$sortArray": {"input": "$revenues", "sortBy": 1}}},
                        "in": {
                            "$arrayElemAt": [
                                "$$sorted",
                                {"$floor": {"$divide": [{"$size": "$$sorted"}, 2]}}
                            ]
                        }
                    }
                }
            }},
            {"$sort": {"median_revenue": -1}},
            {"$limit": 10},
            {"$project": {
                "_id": 0,
                "director_name": "$_id",
                "movie_count": 1,
                "avg_vote": {"$round": ["$avg_vote", 2]},
                "median_revenue": 1
            }}
        ]

        result = list(self.db.movies.aggregate(pipeline))
        for hit in result:
            print(hit)


    def task_2(self):
        print("Task 2")


    def task_3(self):
        print("Task 3")


    def task_4(self):
        print("Task 4")


    def task_5(self):
        print("Task 5")
    

    def task_6(self):
        print("Task 5")


    def task_7(self):
        print("Task 7")


    def task_8(self):
        print("Task 8")


    def task_9(self):
        print("Task 9")


    def task_10(self):
        print("Task 10")


if __name__ == '__main__':
    main()
