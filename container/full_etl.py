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

cols = ['created_at', 'id', 'full_text', 'geo', 'coordinates',
        'retweet_count', 'favorite_count', 'reply_count',
        'quote_count', 'favorited', 'retweeted', 'possibly_sensitive',
        'lang']

response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix="merged/")
for n, key in tqdm(enumerate(response.get('Contents', [])), desc="File: "):
    if n == 0 or n == 1:
        continue
    result = s3_client.get_object(Bucket=BUCKET, Key=key['Key'])
    data = json.loads(result["Body"].read().decode())
    result = ""
    te = [t['raw_data'] for t in data]
    data = ""
    df = pd.DataFrame(te)[cols]
    te = ""
    df['created_at'] = df['created_at'].astype('datetime64[s]')
    df['possibly_sensitive'] = df['possibly_sensitive'].astype('boolean')
    df['lang'] = df['lang'].astype('category')
    df.to_parquet(f's3://merged-tweets/full-sample/full_{n}.parquet')
    df = ""
