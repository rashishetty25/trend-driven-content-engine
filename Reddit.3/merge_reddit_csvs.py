import pandas as pd
import os
from datetime import datetime, timedelta

def merge_reddit_csvs():
    directory = 'Reddit.3'
    now = datetime.now()
    time_threshold = now - timedelta(hours=25)

    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    recent_csv_files = []
    
    for csv_file in csv_files:
        try:
            file_timestamp_str = csv_file.split('_')[3] + csv_file.split('_')[4].split('.')[0]
            file_timestamp = datetime.strptime(file_timestamp_str, '%Y%m%d%H%M')
            if time_threshold <= file_timestamp <= now:
                recent_csv_files.append(csv_file)
        except Exception as e:
            print(f"Skipping file {csv_file} due to error: {e}")
    
    recent_csv_files.sort(key=lambda x: datetime.strptime(x.split('_')[3] + x.split('_')[4].split('.')[0], '%Y%m%d%H%M'), reverse=True)

    master_df = pd.DataFrame()
    for csv_file in recent_csv_files:
        file_path = os.path.join(directory, csv_file)
        current_df = pd.read_csv(file_path)
        
        if master_df.empty:
            master_df = current_df
        else:
            master_df = pd.concat([master_df, current_df]).drop_duplicates(subset='unique_id', keep='last')
    master_df.to_csv('Reddit.3/master_reddit_posts.csv', index=False)

if __name__ == "__main__":
    merge_reddit_csvs()
