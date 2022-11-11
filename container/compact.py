import os
import json
import pandas as pd
from boto3 import client
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_session_token = os.environ['AWS_SESSION_TOKEN']

BUCKET = "merged-tweets"
s3_client = client('s3',
                   aws_access_key_id=aws_access_key_id,
                   aws_secret_access_key=aws_secret_access_key,
                   aws_session_token=aws_session_token)

dfs = []
response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix="full-sample/")
for n, key in tqdm(enumerate(response.get('Contents', [])), desc="File: "):
    if n == 0:
        continue
    df = pd.read_parquet(f"s3://merged-tweets/full-sample/full_{n}.parquet")
    dfs.append(df)

total = pd.concat((dfs))
total.to_parquet("s3://merged-tweets/full-sample/full.parquet")