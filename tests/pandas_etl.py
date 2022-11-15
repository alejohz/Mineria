import pandas as pd
import numpy as np
import timeit
from dotenv import load_dotenv

load_dotenv()


random_state = 42
number = 1
repeat = 10

df = pd.read_parquet('s3://merged-tweets/full-sample/full.parquet')
functions = {
    'read': lambda: pd.read_parquet(
        's3://merged-tweets/full-sample/full.parquet'
    ),
    '10k': lambda: df.sample(10000, random_state=random_state).to_parquet(
        's3://merged-tweets/testing-sample/test_10k.parquet'
    ),
    '100k': lambda: df.sample(100000, random_state=random_state).to_parquet(
        's3://merged-tweets/testing-sample/test_100k.parquet'
    ),
    '500k': lambda: df.sample(500000, random_state=random_state).to_parquet(
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
df_results.to_csv('s3://merged-tweets/results/pandas_etl.csv', index=False)
