!/usr/bin/env python
import snowflake.connector
import os
import sqlite3
import csv
import boto
import ftplib

# Setting your account information
ACCOUNT = 'account'
USER = 'username'
PASSWORD = 'password'

# Connecting to the Snowflake DB
cnx = snowflake.connector.connect(
  user=USER,
  password=PASSWORD,
  account=ACCOUNT,
)

# Using Database, Schema and Warehouse
cnx.cursor().execute("USE warehouse ETL01_WH")
cnx.cursor().execute("USE NOID.DBO")

# S3 Parameters
AWS_ACCESS_KEY_ID = "key_id"
AWS_SECRET_ACCESS_KEY = "Access_Key"
bucket_name = "dm.snowflake"

# Set Parameters
file_date = '2017-01-26'
client_code = 'FREE'
loan_type = 'STML'
file_id_list = '-1' #pass list of fileid's or -1 for all
local_directory = r"d:\temp\deciles\{file_date}\{client_code}\{loan_type}".format(**locals())
ftp_process = False

# Make local directory
if not os.path.exists(local_directory):
    os.makedirs(local_directory)

# Get Export Models
from snowflake.connector import DictCursor
cur = cnx.cursor(DictCursor)
try:
    query = """
            SELECT
              S.MODEL,
              COUNT(1) AS ROWCNT
            FROM
              DBO.SCORE S
            JOIN
              DBO.DECILE D ON D.SCOREID = S.SCOREID
            WHERE
              S.FILEDATE = '{file_date}'
              AND S.CLIENTCODE = '{client_code}'
              AND S.LOANTYPE = '{loan_type}'
              AND (S.FILEID IN ({file_id_list}) OR '{file_id_list}' = '-1')
            GROUP BY
              S.MODEL;
            """.format(**locals())
    model_exports = cur.execute(query).fetchall()
finally:
    cur.close()

# Loop through model exports
for model_export in model_exports:
    #Set Variables
    model = model_export['MODEL']

    # Export to S3
    cur = cnx.cursor()
    query = """
            COPY INTO 
              @DBO.S3_NOID_EXPORT/deciles/{model}.csv
            FROM (
                SELECT
                  S.TRANZACTID AS "TranzactId",
                  S.CreditSeq AS "CreditSeq",
                  S.CRA AS "Cra",
                  S.ZIP AS "Zip",
                  S.FICOV3BASE AS "ficov3base",
                  S.SCORE AS "score",
                  D.DECILE AS "decile"
                FROM
                  DBO.SCORE S
                JOIN
                  DBO.DECILE D ON D.SCOREID = S.SCOREID
                WHERE
                  S.FILEDATE = '{file_date}'
                  AND S.CLIENTCODE = '{client_code}'
                  AND S.LOANTYPE = '{loan_type}'
                  AND (S.FILEID IN ({file_id_list}) OR '{file_id_list}' = '-1')
                  AND S.MODEL = '{model}'
                )
                file_format=(format_name=UTIL_DB.PUBLIC.CSV compression='none'), max_file_size= 5368709120, header=true, single=true, overwrite=true;
            """.format(**locals())
    cur.execute(query)

# Connect to the S3 bucket
from boto.s3.connection import OrdinaryCallingFormat
conn = boto.s3.connect_to_region('us-west-1',
       aws_access_key_id=AWS_ACCESS_KEY_ID,
       aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
       is_secure=True,
       calling_format = boto.s3.connection.OrdinaryCallingFormat()
       )
bucket = conn.get_bucket(bucket_name)

# Copy list of files on S3
files = bucket.list(prefix="noid/exports/deciles/")
for l in files:
    key_string = str(l.key)
    if ".csv" in key_string:
        local_path = local_directory+"\\"+key_string.split("/")[-1]
        l.get_contents_to_filename(local_path)
        bucket.delete_key(l.key)

# Create dmnas04 directory if not exists
dest_directory = r"""\\dmnas04\NoidScoreData\Adrian Scores\{file_date}\{client_code}\{loan_type}""".format(**locals())
if not os.path.exists(dest_directory):
    os.makedirs(dest_directory)

# Ftp files
if ftp_process:
    for local_file in os.listdir(local_directory):
        ftp = ftplib.FTP("dmnas04")
        ftp.login("email@abc.com", "password")
        ftp_dest_directory = r"""path""".format(**locals())
        ftp.cwd(ftp_dest_directory)
        local_path = local_directory+"\\"+local_file
        ftp_file = open(local_path, 'rb')
        ftp.storbinary("STOR " + local_file, ftp_file)
        ftp_file.close()
        ftp.quit()
        # delete temp file
        os.remove(local_path)
    # remove empty directories
    os.removedirs(local_directory)
