import json
from boto3 import client
from tqdm import tqdm
import os

ACCESS_KEY_ID = os.environ["ACCESS_KEY_ID"]
SECRET_ACCESS_KEY = os.environ["SECRET_ACCESS_KEY"]
AWS_SESSION_TOKEN = os.environ["AWS_SESSION_TOKEN"]
REGION_NAME = os.environ["AWS_REGION"]

client = client("s3",
                aws_access_key_id=ACCESS_KEY_ID,
                aws_secret_access_key=SECRET_ACCESS_KEY,
                aws_session_token=AWS_SESSION_TOKEN,
                region_name=REGION_NAME)
BUCKET = "ukraine-war-tweets"

paginator = client.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=BUCKET)

data = json.loads(client.get_object(Bucket="merged-tweets", Key="merged/merged.json")["Body"].read().decode())
for page in tqdm(pages, desc="Page"):
    for key in page["Contents"]:
        text = client.get_object(Bucket=BUCKET, Key=key["Key"])["Body"].read().decode("utf-8")
        if text:
            data.append(json.loads(text))
        client.put_object(
            Body=json.loads(text),
            Bucket="merged-tweets",
            Key=key["Key"]
            )
        client.delete_object(
            Bucket=BUCKET,
            Key=key["Key"]
            )
        

client.put_object(
     Body=json.dumps(data),
     Bucket="merged-tweets",
     Key="merged/merged.json"
)
