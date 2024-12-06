import pandas as pd
import os
from datetime import datetime

def merge_reddit_csvs():
    directory = 'Reddit.3'
    
    # Define the threshold timestamp for November 6th, 2024 at 00:00
    threshold_timestamp = datetime.strptime('20241106_0000', '%Y%m%d_%H%M')

    # Get all CSV files in the directory
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    
    # Filter files that are created after the threshold timestamp
    recent_csv_files = []
    
    for csv_file in csv_files:
        try:
            # Extract timestamp from the filename (e.g., "20241101_1834")
            timestamp_str = csv_file.split('_')[3] + csv_file.split('_')[4].split('.')[0]
            file_timestamp = datetime.strptime(timestamp_str, '%Y%m%d%H%M')
            
            # Check if the file timestamp is after the threshold
            if file_timestamp > threshold_timestamp:
                recent_csv_files.append(csv_file)
        except Exception as e:
            print(f"Skipping file {csv_file} due to error: {e}")
    
    # Sort the filtered files by timestamp in descending order
    recent_csv_files.sort(key=lambda x: datetime.strptime(x.split('_')[3] + x.split('_')[4].split('.')[0], '%Y%m%d%H%M'), reverse=True)

    master_df = pd.DataFrame()
    
    # Merge all the selected files into a single DataFrame
    for csv_file in recent_csv_files:
        file_path = os.path.join(directory, csv_file)
        current_df = pd.read_csv(file_path)
        
        if master_df.empty:
            master_df = current_df
        else:
            master_df = pd.concat([master_df, current_df]).drop_duplicates(subset='unique_id', keep='last')

    master_df.to_csv('Reddit.3/master_reddit.csv', index=False)

if __name__ == "__main__":
    merge_reddit_csvs()
