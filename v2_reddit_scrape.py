import praw
import pandas as pd
from datetime import datetime, timedelta
import os
import json

# Function to load previous data from a JSON file
def load_previous_data(filename):
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            return json.load(file)
    return {}

# Function to save current data to a JSON file
def save_current_data(filename, data):
    with open(filename, 'w') as file:
        json.dump(data, file)

# Function to append new value to the key
def append_value(key, new_value, data):
    if key not in data:
        data[key] = []  # Initialize to an empty list if the key does not exist
    elif not isinstance(data[key], list):
        data[key] = [data[key]]  # Convert to list if it's not already a list
    data[key].append(new_value)
    
    # Keep only the last 24 values
    if len(data[key]) > 24:
        data[key].pop(0)  # Remove the oldest value
        
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

    # Load previous upvotes and comments data
    previous_upvotes = load_previous_data('Reddit.2/previous_upvotes.json')
    previous_comments = load_previous_data('Reddit.2/previous_comments.json')

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
    for submission in subreddit.top(time_filter='day', limit=1000):
        flair_tag = submission.link_flair_text

        if flair_tag and flair_tag.split(':')[-1].strip() in excluded_flairs:
            continue

        # Calculate post age
        total_seconds = (datetime.utcnow() - datetime.utcfromtimestamp(submission.created_utc)).total_seconds()
        post_age_days = total_seconds // 86400
        post_age_time = str(timedelta(seconds=total_seconds))

        # Format post_age
        post_age = f"{int(post_age_days)} days" if post_age_days > 0 else "0 days"

        post_content = submission.selftext

        # Collect top 10 comments
        top_comments = [comment.body for comment in submission.comments.list()[:10] if comment.author and comment.author.name != "AutoModerator"]

        # Calculate growth rates
        current_upvotes = submission.ups
        current_comments = submission.num_comments

        previous_upvote_count = previous_upvotes.get(submission.id, current_upvotes)
        previous_comment_count = previous_comments.get(submission.id, current_comments)

        # Calculate growth rates
        upvotes_growth_rate = current_upvotes - previous_upvote_count
        comments_growth_rate = current_comments - previous_comment_count

        # Store current values for future reference
        append_value(submission.id, current_upvotes, previous_upvotes)
        append_value(submission.id, current_comments, previous_comments)

        post_data = {
            'unique_id': submission.id,
            'post_heading': submission.title,
            'link_url': submission.url,
            'post_url': f'https://www.reddit.com{submission.permalink}',
            'publish_time': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d'),
            'post_age': post_age,
            'post_content': post_content,
            'top_comments': top_comments,
            'upvotes': current_upvotes,
            'comments': current_comments,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'tag': flair_tag.split(':')[-1] if flair_tag else None,
            'upvotes_growth_rate': upvotes_growth_rate,
            'comments_growth_rate': comments_growth_rate
        }
        posts_data.append(post_data)

    # Create a DataFrame from the collected data
    reddit_df = pd.DataFrame(posts_data)

    # Save the CSV file without seconds in the filename
    log_filename = f'Reddit.2/reddit_f1_log_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    reddit_df.to_csv(log_filename, index=False)

    # Save the updated upvotes and comments data for the next run
    save_current_data('Reddit.2/previous_upvotes.json', previous_upvotes)
    save_current_data('Reddit.2/previous_comments.json', previous_comments)

if __name__ == "__main__":
    main()
