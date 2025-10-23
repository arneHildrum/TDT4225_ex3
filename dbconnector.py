from pymongo import MongoClient, version


class DbConnector:
    """
    mongodb://mongodb:secret123@localhost:27017
    """

    def __init__(self,
                 DATABASE='DATABASE_NAME',
                 HOST="tdt4225-xx.idi.ntnu.no",
                 USER="mongodb",
                 PASSWORD="secret123"):
        uri = "mongodb://%s:%s@%s/%s" % (USER, PASSWORD, HOST, DATABASE)
        # Connect to the databases
        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

        # get database information
        print("You are connected to the database:", self.db.name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        # close the cursor
        # close the DB connection
        self.client.close()
        print("\n-----------------------------------------------")
        print("Connection to %s-db is closed" % self.db.name)
