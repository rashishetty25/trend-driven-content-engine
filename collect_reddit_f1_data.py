import praw
import pandas as pd
from datetime import datetime
import os

def main():
    reddit = praw.Reddit(
        client_id=os.environ['CLIENT_ID'],
        client_secret=os.environ['CLIENT_SECRET'],
        password=os.environ['PASSWORD'],
        user_agent=os.environ['USER_AGENT'],
        username=os.environ['USERNAME'],
    )

    # Create Reddit folder if it doesn't exist
    if not os.path.exists('Reddit'):
        os.makedirs('Reddit')

    subreddit = reddit.subreddit('formula1')
    posts_data = []

    for submission in subreddit.new(limit=1000):
        post_data = {
            'unique_id': submission.id,
            'post_heading': submission.title,
            'tag': submission.link_flair_text,
            'upvotes': submission.ups,
            'comments': submission.num_comments,
            'URL': submission.url,
            'publish_time': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S')
        }
        posts_data.append(post_data)

    new_df = pd.DataFrame(posts_data)

    # Add a new column for the timestamp of data collection
    new_df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    hourly_filename = f'Reddit/reddit_f1_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    new_df.to_csv(hourly_filename, index=False)

    master_filename = 'Reddit/master_reddit_f1_data.csv'
    if os.path.exists(master_filename):
        master_df = pd.read_csv(master_filename)
    else:
        master_df = pd.DataFrame(columns=['unique_id', 'post_heading', 'tag', 'upvotes', 'comments', 
                                           'URL', 'publish_time', 'timestamp', 
                                           '24_hour_popularity_upvote', 
                                           '24_hour_popularity_comment'])

    for index, row in new_df.iterrows():
        unique_id = row['unique_id']
        if unique_id in master_df['unique_id'].values:
            existing_row = master_df.loc[master_df['unique_id'] == unique_id].iloc[0]
            upvote_change = row['upvotes'] - existing_row['upvotes']
            comment_change = row['comments'] - existing_row['comments']
            master_df.loc[master_df['unique_id'] == unique_id, 'upvotes'] = row['upvotes']
            master_df.loc[master_df['unique_id'] == unique_id, 'comments'] = row['comments']
            master_df.loc[master_df['unique_id'] == unique_id, '24_hour_popularity_upvote'] = upvote_change
            master_df.loc[master_df['unique_id'] == unique_id, '24_hour_popularity_comment'] = comment_change
        else:
            new_row = pd.DataFrame([{
                'unique_id': row['unique_id'],
                'post_heading': row['post_heading'],
                'tag': row['tag'],
                'upvotes': row['upvotes'],
                'comments': row['comments'],
                'URL': row['URL'],
                'publish_time': row['publish_time'],
                'timestamp': row['timestamp'],
                '24_hour_popularity_upvote': '',
                '24_hour_popularity_comment': ''
            }])
            master_df = pd.concat([master_df, new_row], ignore_index=True)

    master_df.to_csv(master_filename, index=False)

if __name__ == "__main__":
    main()
