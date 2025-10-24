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
        collections = self.client['movies'].list_collection_names()
        print(collections)


    def task_1():
        print("Task 1")


    def task_2():
        print("Task 2")


    def task_3():
        print("Task 3")


    def task_4():
        print("Task 4")


    def task_5():
        print("Task 5")
    

    def task_6():
        print("Task 5")


    def task_7():
        print("Task 7")


    def task_8():
        print("Task 8")


    def task_9():
        print("Task 9")


    def task_10():
        print("Task 10")


    def run(self):
        task = 100
        while(task != 0):
            task = int(input("Enter task number (0-10): "))
            if task == 0:
                print("Shutting down program.......")
            else:
                match task:
                    case 1: Program.task_1()
                    case 2: Program.task_2()
                    case 3: Program.task_3()
                    case 4: Program.task_4()
                    case 5: Program.task_5()
                    case 6: Program.task_6()
                    case 7: Program.task_7()
                    case 8: Program.task_8()
                    case 9: Program.task_9()
                    case 10: Program.task_10()

def main():
    program = None
    try:
        program = Program()
        #program.setup()
        program.show_coll()
        program.run()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
