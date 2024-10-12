import pandas as pd
import os
from datetime import datetime, timedelta

# Function to merge CSVs and calculate popularity metrics
def merge_reddit_csvs():
    directory = 'Reddit'
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

    # Initialize an empty DataFrame to hold merged data and a dictionary for popularity tracking
    master_df = pd.DataFrame()
    popularity_dict = {}

    for csv_file in recent_csv_files:
        # Load the current CSV
        file_path = os.path.join(directory, csv_file)
        current_df = pd.read_csv(file_path)

        # Update popularity_dict
        for _, row in current_df.iterrows():
            unique_id = row['unique_id']
            upvotes = row['upvotes']
            comments = row['comments']
            
            # Initialize dictionary if unique_id is not present
            if unique_id not in popularity_dict:
                popularity_dict[unique_id] = {
                    'initial_upvotes': upvotes,
                    'initial_comments': comments,
                    'latest_upvotes': upvotes,
                    'latest_comments': comments
                }
            else:
                # Update latest values
                popularity_dict[unique_id]['latest_upvotes'] = max(popularity_dict[unique_id]['latest_upvotes'], upvotes)
                popularity_dict[unique_id]['latest_comments'] = max(popularity_dict[unique_id]['latest_comments'], comments)

        # Drop duplicates based on unique_id before appending
        current_df = current_df.drop_duplicates(subset=['unique_id'])

        # Append current_df to master_df directly for later processing
        master_df = pd.concat([master_df, current_df], ignore_index=True)

    # Remove duplicates from master_df based on unique_id and keep the latest record
    master_df = master_df.sort_values('timestamp', ascending=False).drop_duplicates(subset=['unique_id'], keep='first')

    # Convert popularity_dict to DataFrame
    popularity_df = pd.DataFrame.from_dict(popularity_dict, orient='index')

    # Merge with master_df
    master_df = master_df.merge(popularity_df, left_on='unique_id', right_index=True, how='left')

    # Calculate popularity metrics in a vectorized way
    master_df['popularity_upvote'] = master_df['latest_upvotes'] - master_df['initial_upvotes']
    master_df['popularity_comment'] = master_df['latest_comments'] - master_df['initial_comments']

    # Drop the intermediate columns used for calculation
    master_df.drop(columns=['latest_upvotes', 'latest_comments'], inplace=True)

    # Save the master DataFrame to a new CSV file
    master_df.to_csv('Reddit/master_reddit_posts.csv', index=False)

# Run the function
if __name__ == "__main__":
    merge_reddit_csvs()
