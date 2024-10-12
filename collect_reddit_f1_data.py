import praw
import pandas as pd
from datetime import datetime, timedelta
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

    # Collect posts
    for submission in subreddit.new(limit=1000):  # Scraping 1000 posts
        # Calculate post age
        post_age = timedelta(seconds=(datetime.utcnow() - datetime.utcfromtimestamp(submission.created_utc)).total_seconds())

        post_data = {
            'unique_id': submission.id,
            'post_heading': submission.title,
            'URL': submission.url,
            'publish_time': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d'),  # Remove time
            'post_age': str(post_age),
            'upvotes': submission.ups,
            'comments': submission.num_comments,
            'timestamp': datetime.now().strftime('%Y-%m-%d'),  # Remove time
            'tag': submission.link_flair_text  # Collecting the flair tag
        }
        posts_data.append(post_data)

    # Create a DataFrame from the collected data
    reddit_df = pd.DataFrame(posts_data)

    # Remove the text before the colon in the 'tag' column
    reddit_df['tag'] = reddit_df['tag'].str.split(':').str[-1].str.strip()  # Keep only the text after the last colon and strip whitespace

    # Define flair tags to exclude
    excluded_flairs = {
        'Off-Topic',
        'Misc',
        'Social Media',
        'Video',
        'Poster',
        'Photo',
        'Daily Discussion',
        'Discussion',
        'AMA',
        'Satire',
        'Automated Removal'
    }

    # Filter out non-important tags
    filtered_reddit_df = reddit_df[~reddit_df['tag'].isin(excluded_flairs)]

    # Save the CSV file without seconds in the filename
    log_filename = f'Reddit/reddit_f1_log_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    filtered_reddit_df.to_csv(log_filename, index=False)

if __name__ == "__main__":
    main()
