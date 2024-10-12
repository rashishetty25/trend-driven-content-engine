import pandas as pd
import os
from datetime import datetime, timedelta

# Function to merge CSVs and calculate popularity metrics
def merge_reddit_csvs():
    directory = 'Reddit'
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

        # Merge current_df into master_df on unique_id, keeping the most recent upvotes and comments
        if master_df.empty:
            master_df = current_df
        else:
            master_df = pd.merge(master_df, current_df, on='unique_id', how='outer', suffixes=('', '_new'))
            # Update upvotes and comments with the most recent values
            master_df['upvotes'] = master_df[['upvotes', 'upvotes_new']].bfill(axis=1).iloc[:, 0]
            master_df['comments'] = master_df[['comments', 'comments_new']].bfill(axis=1).iloc[:, 0]
            # Drop unnecessary columns
            master_df.drop(columns=[col for col in master_df.columns if 'new' in col], inplace=True)

    # Calculate popularity metrics
    first_upvotes = master_df.groupby('unique_id')['upvotes'].transform('first')
    first_comments = master_df.groupby('unique_id')['comments'].transform('first')

    # Calculate popularity as the difference from the first recorded values
    master_df['popularity_upvote'] = master_df['upvotes'] - first_upvotes
    master_df['popularity_comment'] = master_df['comments'] - first_comments

    # Save the master DataFrame to a new CSV file
    master_df.to_csv('Reddit/master_reddit_posts.csv', index=False)

# Run the function
if __name__ == "__main__":
    merge_reddit_csvs()
