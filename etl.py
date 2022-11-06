import pandas as pd
import json

from pathlib import Path

p = Path("/home/ahenao/Documents/twitter_selenium/TweetScraper/Output/")

tweets = (p / "MDGBI_tweets.json")
# users (p.raw / "Scribe_users_merged.json")

with open(tweets, 'r') as f:
    data = f.read()
    data_json = json.loads(data)
te = [t['raw_data'] for t in data_json]
df = pd.DataFrame(te)

cols = ['created_at', 'id', 'full_text', 'geo', 'coordinates',
        'retweet_count', 'favorite_count', 'reply_count',
        'quote_count', 'favorited', 'retweeted', 'possibly_sensitive',
        'lang']

df[cols].to_parquet(Path("./data/") / "tweets_small_sample.parquet")
