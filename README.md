# TDT4225 exercise 3
This repository contains the code for exercise 2 in the course TDT4225 for the 2025 fall. The code is spesific for running mysql on a docker instance. 

Author: Arne Hildrum
## Requirements 
- Windows 11
- Docker for windows
- Python 3.13.7
Other versions of python may also work, but this is a windows spesific setup.

## Deployment
As long as the requirements are met, these should be the only commands needed to deploy the program:
```ps1
docker run -d mongodb/mongodb-enterprise-server:latest --name mongodb -p 27017:27017 -e MONGO_INITDB_ROOT_USERNAME=root -e MONGO_INITDB_ROOT_PASSWORD=secret123
py .\data_cleaning.py
py .\program.py
```
## Data
This program utilizes the following data sets:
- credits.csv
- keywords.csv
- links.csv
- movies_metadata.csv
- ratings.csv
