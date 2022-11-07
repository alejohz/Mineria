import os
import json
import pandas as pd
from boto3 import client
from dotenv import load_dotenv

load_dotenv()

aws_access_key_id = os.environ['aws_access_key_id']
aws_secret_access_key = os.environ['aws_secret_access_key']
aws_session_token = os.environ['aws_session_token']

BUCKET = "merged-tweets"
KEY = "testing-sample/merged.json"
s3_client = client('s3',
                   aws_access_key_id=aws_access_key_id,
                   aws_secret_access_key=aws_secret_access_key,
                   aws_session_token=aws_session_token)

result = s3_client.get_object(Bucket=BUCKET, Key=KEY)
data = json.loads(result["Body"].read().decode())

cols = ['created_at', 'id', 'full_text', 'geo', 'coordinates',
        'retweet_count', 'favorite_count', 'reply_count',
        'quote_count', 'favorited', 'retweeted', 'possibly_sensitive',
        'lang']
te = [t['raw_data'] for t in data]
df = pd.DataFrame(te)[cols]

df['created_at'] = df['created_at'].astype('datetime64[s]')
df['possibly_sensitive'] = df['possibly_sensitive'].astype('boolean')
df['lang'] = df['lang'].astype('category')

df.to_parquet('s3://merged-tweets/testing-sample/test.parquet')
