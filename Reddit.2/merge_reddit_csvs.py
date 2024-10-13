import pandas as pd
import os
from datetime import datetime, timedelta

# Function to merge CSVs without duplicates
def merge_reddit_csvs():
    directory = 'Reddit.2'
    # Get the current time and the time 24 hours ago
    now = datetime.now()
    time_threshold = now - timedelta(hours=24)

    # Get all CSV files in the directory
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]

    # Filter CSV files created in the last 24 hours
    recent_csv_files = [
        f for f in csv_files if datetime.fromtimestamp(os.path.getmtime(os.path.join(directory, f))) > time_threshold
    ]

    # Sort files by their modification time to process from oldest to newest
    recent_csv_files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)))

    # Initialize a DataFrame to hold merged data
    master_df = pd.DataFrame()

    for csv_file in recent_csv_files:
        # Load the current CSV
        file_path = os.path.join(directory, csv_file)
        current_df = pd.read_csv(file_path)

        # Merge current_df into master_df on unique_id without duplicates
        master_df = pd.concat([master_df, current_df]).drop_duplicates(subset='unique_id', keep='last')

    # Save the master DataFrame to a new CSV file
    master_df.to_csv('Reddit.2/master_reddit_posts.csv', index=False)

# Run the function
if __name__ == "__main__":
    merge_reddit_csvs()
