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

    if not os.path.exists('Reddit.2'):
        os.makedirs('Reddit.2')

    subreddit = reddit.subreddit('formula1')
    posts_data = []

    # Define flair tags to exclude
    excluded_flairs = {
        'Off-Topic',
        'Misc',
        'Poster',
        'Photo',
        'Daily Discussion',
        'AMA',
        'Satire',
        'Automated Removal'
    }

    # Collect posts
    for submission in subreddit.top(time_filter='day', limit=1000):  # On race weekends, you can set to "hour"
        flair_tag = submission.link_flair_text

        # Skip posts with excluded flair tags
        if flair_tag and flair_tag.split(':')[-1].strip() in excluded_flairs:
            continue  # Skip to the next post if the flair tag is excluded

        # Calculate post age
        total_seconds = (datetime.utcnow() - datetime.utcfromtimestamp(submission.created_utc)).total_seconds()
        post_age_days = total_seconds // 86400  # Calculate full days
        post_age_time = str(timedelta(seconds=total_seconds))  # Get full timedelta
        
        # Format post_age
        post_age = f"{int(post_age_days)} days" if post_age_days > 0 else "0 days"

        post_content = submission.selftext  # or submission.url if it's a link post

        # Collect top 10 comments, excluding AutoModerator
        top_comments = [comment.body for comment in submission.comments.list()[:10] if comment.author and comment.author.name != "AutoModerator"]
        
        post_data = {
            'unique_id': submission.id,
            'post_heading': submission.title,
            'link_url': submission.url,
            'post_url': f'https://www.reddit.com{submission.permalink}',  
            'publish_time': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d'),  # Keep date
            'post_age': post_age, 
            'post_content': post_content, 
            'top_comments': top_comments, 
            'upvotes': submission.ups,
            'comments': submission.num_comments,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'tag': flair_tag,
        }
        posts_data.append(post_data)

    # Create a DataFrame from the collected data
    reddit_df = pd.DataFrame(posts_data)

    # Save the CSV file without seconds in the filename
    log_filename = f'Reddit.2/reddit_f1_log_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    reddit_df.to_csv(log_filename, index=False)

if __name__ == "__main__":
    main()
