# import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs 

def run_twitter_etl():
    csv_file_path = "s3://ali-airflow-bucket-twitter-data/tweets.csv"
    try:
        df = pd.read_csv(csv_file_path)
        datetime_format = "%d/%m/%Y %H:%M"
        df['date_time'] = pd.to_datetime(df['date_time'],format=datetime_format)
        # Separate date and time components into separate columns
        df['date'] = df['date_time'].dt.date
        df['time'] = df['date_time'].dt.time
        selected_columns_df = df[['author', 'content', 'date', 'time', 'number_of_likes', 'number_of_shares']]
        selected_columns_df.to_csv("s3://ali-airflow-bucket-twitter-data/Modified_Tweets.csv")
    except FileNotFoundError:
        print(f"Error: File '{csv_file_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

run_twitter_etl()