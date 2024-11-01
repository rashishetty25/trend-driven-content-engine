import pandas as pd
import os
from datetime import datetime, timedelta

def merge_reddit_csvs():
    directory = 'Reddit.3'
    now = datetime.now()
    time_threshold = now - timedelta(hours=24)
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    recent_csv_files = [f for f in csv_files if datetime.fromtimestamp(os.path.getmtime(os.path.join(directory, f))) > time_threshold]
    recent_csv_files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)))
    master_df = pd.DataFrame()

    for csv_file in recent_csv_files:
        file_path = os.path.join(directory, csv_file)
        current_df = pd.read_csv(file_path)
        master_df = pd.concat([master_df, current_df]).drop_duplicates(subset='unique_id', keep='last')

    master_df.to_csv('Reddit.3/master_reddit_posts.csv', index=False)

if __name__ == "__main__":
    merge_reddit_csvs()
