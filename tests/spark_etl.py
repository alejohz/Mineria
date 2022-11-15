import pandas as pd
import numpy as np
import timeit
import os
from dotenv import load_dotenv
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('aws_access_key_id'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('aws_secret_access_key'))
conf.set('spark.hadoop.fs.s3a.session.token', os.getenv('aws_session_token'))

spark = SparkSession.builder.config(conf=conf).getOrCreate()

load_dotenv()


random_state = 42
number = 1
repeat = 10

df = spark.read.parquet('s3://merged-tweets/full-sample/full.parquet')
count = df.count()
functions = {
    'read': lambda: pd.read_parquet(
        's3://merged-tweets/full-sample/full.parquet'
    ),
    '10k': lambda: df.sample(10000 / count, random_state=random_state).write.parquet(
        's3://merged-tweets/testing-sample/test_10k.parquet'
    ),
    '100k': lambda: df.sample(100000 / count, random_state=random_state).write.parquet(
        's3://merged-tweets/testing-sample/test_100k.parquet'
    ),
    '500k': lambda: df.sample(500000 / count, random_state=random_state).write.parquet(
        's3://merged-tweets/testing-sample/test_500k.parquet'
    ),
    'full': lambda: df.write.parquet(
        's3://merged-tweets/testing-sample/test_full.parquet'
    ),
}

df_results = pd.DataFrame()
for function in functions:
    time = timeit.repeat(functions[function], number=number, repeat=repeat)
    df_results.loc[0, f'{function}_avg'] = np.mean(time)
    df_results.loc[0, f'{function}_std'] = np.std(time)
df_results.to_csv('s3://merged-tweets/results/spark_etl.csv', index=False)
