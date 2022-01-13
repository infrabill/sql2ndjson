# sql2ndjson
Takes mysql tables, converts them to ndjson files, then uploads them to s3. The ndjson files will be stored with in the root directory before they are uploaded to s3.

## Built With
- pandas=1.3.5
- mysql-connector-python=8.0.27
- boto3=1.20.26
- python-dotenv=0.19.2

## Getting Started
To set up this project locally, follow these steps.

## Prerequisites
- python3.9
- pip3

## Installation
1. Clone the repo
2. Install package dependencies using PIP

```
pip3 install -r requirements.txt
```
3. Open/Create a .env file in the root folder and provide the follwing credentials:
```
workdir = 'working directory'

PROD = 'database server'
PROD_user = 'db user'
PROD_pass = 'db password'

s3bucket = 's3 bucket name'

ACCESS_KEY = 'aws access key'
SECRET_KEY = 'aws secret key'
```

4. Run
```
python3 sql2ndjson.py
```

