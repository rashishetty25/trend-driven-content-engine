import pandas as pd
import os
from datetime import datetime

# Function to merge CSVs and calculate popularity metrics
def merge_reddit_csvs():
    # Directory containing the CSV files
    directory = 'Reddit'
    
    # Get all CSV files in the directory
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    
    # Sort files by their names to process from oldest to newest
    csv_files.sort()

    # Initialize a DataFrame to hold merged data
    master_df = pd.DataFrame()

    for csv_file in csv_files:
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
    master_df['popularity_upvote'] = master_df['upvotes'] - master_df.groupby('unique_id')['upvotes'].transform('first')
    master_df['popularity_comment'] = master_df['comments'] - master_df.groupby('unique_id')['comments'].transform('first')

    # Save the master DataFrame to a new CSV file
    master_df.to_csv('Reddit/master_reddit_posts.csv', index=False)

# Run the function
if __name__ == "__main__":
    merge_reddit_csvs()
