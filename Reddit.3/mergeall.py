import pandas as pd
from datetime import datetime
import os

def merge_reddit_csvs():
    directory = 'Reddit.3'

    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]

    csv_files.sort(key=lambda x: datetime.strptime(x.split('_')[3] + x.split('_')[4].split('.')[0], '%Y%m%d%H%M'), reverse=True)

    master_df = pd.DataFrame()

    for csv_file in csv_files:
        file_path = os.path.join(directory, csv_file)
        current_df = pd.read_csv(file_path)
        
        if master_df.empty:
            master_df = current_df
        else:
            master_df = pd.concat([master_df, current_df]).drop_duplicates(subset='unique_id', keep='last')

    master_df.to_csv('Reddit.3/master_reddit.csv', index=False)

if __name__ == "__main__":
    merge_reddit_csvs()
