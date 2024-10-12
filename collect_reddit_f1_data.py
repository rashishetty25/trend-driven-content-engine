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

    # List of non-important link flair tags to filter out
    non_important_flair = [
        'Off-Topic', 'Misc', 'Social Media', 'Video', 'Poster', 'Photo',
        'daily discussion', 'Discussion', 'AMA', 'Satire', ':post-moderator-removal:', 'Automated Removal'
    ]

    for submission in subreddit.new(limit=20):  # Scraping 20 posts
        # Print flair for debugging
        flair_text = submission.link_flair_text

        # Check if the flair text is not None and not in the non-important list
        if flair_text is not None and flair_text not in non_important_flair:
            # Calculate post age
            post_age = timedelta(seconds=(datetime.utcnow() - datetime.utcfromtimestamp(submission.created_utc)).total_seconds())

            post_data = {
                'unique_id': submission.id,
                'post_heading': submission.title,
                'URL': submission.url,
                'publish_time': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                'post_age': str(post_age),
                'upvotes': submission.ups,
                'comments': submission.num_comments,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'tag': flair_text  # Collecting the flair tag
            }
            posts_data.append(post_data)

    # Create a DataFrame from the collected data
    reddit_df = pd.DataFrame(posts_data)

    # Save the CSV file without seconds in the filename
    log_filename = f'Reddit/reddit_f1_log_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    reddit_df.to_csv(log_filename, index=False)

if __name__ == "__main__":
    main()
