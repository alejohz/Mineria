import dask.dataframe as dd
from dask.distributed import Client
from dask_cloudprovider.aws import EC2Cluster
import pandas as pd
import numpy as np
import timeit
import configparser
import os
import contextlib
from dotenv import load_dotenv

load_dotenv()


def get_aws_credentials():
    parser = configparser.RawConfigParser()
    parser.read(os.path.expanduser('~/.aws/config'))
    config = parser.items('default')
    parser.read(os.path.expanduser('~/.aws/credentials'))
    credentials = parser.items('mining')
    all_credentials = {key.upper(): value for key, value in [*config, *credentials]}
    with contextlib.suppress(KeyError):
        all_credentials["AWS_REGION"] = all_credentials.pop("REGION")
    all_credentials['EXTRA_PIP_PACKAGES'] = 's3fs pyarrow plotly'
    return all_credentials


cluster = EC2Cluster(region='us-east-1', n_workers=1, security=False, env_vars=get_aws_credentials())
client = Client(cluster)
# client = Client("174.129.112.139:8786")

random_state = 42
number = 1
repeat = 10

# aws_access_key_id = os.environ['aws_access_key_id']
# aws_secret_access_key = os.environ['aws_secret_access_key']
# aws_session_token = os.environ['aws_session_token']
# kwargs = dict(keystring=aws_access_key_id,
#               secretstring=aws_secret_access_key,
#               tokenstring=aws_session_token)
df: dd.DataFrame = dd.read_parquet('s3://merged-tweets/full-sample/full.parquet')
df['geo'] = df['geo'].astype(str)
df['coordinates'] = df['coordinates'].astype(str)
count = len(df.index)
functions = {
    'read': lambda: dd.read_parquet(
        's3://merged-tweets/full-sample/full.parquet'
    ),
    '10k': lambda: df.sample(frac=10000 / count, random_state=random_state).to_parquet(
        's3://merged-tweets/testing-sample/test_10k.parquet'
    ),
    '100k': lambda: df.sample(frac=100000 / count, random_state=random_state).to_parquet(
        's3://merged-tweets/testing-sample/test_100k.parquet'
    ),
    '500k': lambda: df.sample(frac=500000 / count, random_state=random_state).to_parquet(
        's3://merged-tweets/testing-sample/test_500k.parquet'
    ),
    'full': lambda: df.to_parquet(
        's3://merged-tweets/testing-sample/test_full.parquet'
    ),
}

df_results = pd.DataFrame()
for function in functions:
    time = timeit.repeat(functions[function], number=number, repeat=repeat)
    df_results.loc[0, f'{function}_avg'] = np.mean(time)
    df_results.loc[0, f'{function}_std'] = np.std(time)
df_results.to_csv('s3://merged-tweets/results/dask_etl_1_worker.csv', index=False)

cluster.close()
