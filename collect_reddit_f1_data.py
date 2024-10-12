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

    for submission in subreddit.new(limit=1000):
        # Collect top 10 comments content
        submission.comments.replace_more(limit=0)
        top_comments = [comment.body for comment in submission.comments.list()[:10]]
        top_comments_content = " | ".join(top_comments)

        # Calculate post age
        post_age = timedelta(seconds=(datetime.utcnow() - datetime.utcfromtimestamp(submission.created_utc)).total_seconds())

        post_data = {
            'unique_id': submission.id,
            'post_heading': submission.title,
            'post_content': submission.selftext,
            'top_10_comments': top_comments_content,
            'URL': submission.url,
            'publish_time': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
            'post_age': str(post_age),
            'current_upvotes': submission.ups,
            'current_comments': submission.num_comments,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        posts_data.append(post_data)

    # Create a DataFrame from the collected data
    reddit_df = pd.DataFrame(posts_data)

    # Save the CSV file without seconds in the filename
    log_filename = f'Reddit/reddit_f1_log_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    reddit_df.to_csv(log_filename, index=False)

if __name__ == "__main__":
    main()
