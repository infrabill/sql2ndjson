import os
import pandas as pd
import mysql.connector
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv
import logging
from datetime import datetime, time

# Load and define path of .env file
load_dotenv()

#Date for filenaming
date = datetime.now().strftime("%d-%m-%Y")

#Logging 
logging.basicConfig(filename='/var/log/sql2ndjson/sql2ndjson.log',
                    format='%(asctime)s %(message)s', encoding='utf-8', level=logging.DEBUG)

workdir = os.getenv('workdir')
dbserver = os.getenv('PROD')
db = os.getenv('PRODdb')
dbuser = os.getenv('prodUSER')
dbpass = os.getenv('prodPASS')
s3bucket = os.getenv('s3bucket')
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')

# boto3 s3 config
config = TransferConfig(multipart_threshold=1024 * 25,
                        max_concurrency=10,
                        multipart_chunksize=1024 * 25,
                        use_threads=True)

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    try:
        s3.upload_file(local_file, bucket, s3_file, ExtraArgs={
                       'ACL': 'bucket-owner-full-control'}, Config=config)
        logging.info("Upload Successful")
        return True
    except FileNotFoundError:
        logging.error("The file was not found")
        return False
    except ClientError as e:
        logging.error(e)
        return False
    except NoCredentialsError:
        logging.error("Credentials not available")
        return False

conn = mysql.connector.connect(
    host=dbserver,
    user=dbuser,
    password=dbpass,
    database=db
)

#List of tables
tables = ['<table_name_here>']

for tbl in tables:
    cur = conn.cursor(buffered=True)
    sql = 'select * from {}'.format(tbl)
    try:
        exec=cur.execute(sql)
    except:
        logging.error(exec)
    df_sql_data = pd.DataFrame.from_records(
        cur.fetchall(), columns=[i[0] for i in cur.description])
    df_sql_data.to_json(f'{tbl}.ndjson', orient='records', lines=True)
    df_sql_data= pd.DataFrame.from_records(cur.fetchall(), columns=[i[0] for i in cur.description])
    cur.close()
    uploaded = upload_to_aws(
        f'{tbl}.ndjson', s3bucket, f'{tbl}{date}.ndjson')

