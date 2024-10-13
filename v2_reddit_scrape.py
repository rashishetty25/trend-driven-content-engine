import os
import json
import praw
import pandas as pd
from datetime import datetime, timedelta

# Get the absolute path to the directory where the script is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Function to load previous data from a JSON file
def load_previous_data(filename):
    filepath = os.path.join(BASE_DIR, filename)  # Build the full file path
    if os.path.exists(filepath):
        with open(filepath, 'r') as file:
            return json.load(file)
    return {}

# Function to save current data to a JSON file
def save_current_data(filename, data):
    filepath = os.path.join(BASE_DIR, filename)  # Build the full file path
    with open(filepath, 'w') as file:
        json.dump(data, file)
    print(f"Data saved to {filename}: {data}")

def main():
    reddit = praw.Reddit(
        client_id=os.environ['CLIENT_ID'],
        client_secret=os.environ['CLIENT_SECRET'],
        password=os.environ['PASSWORD'],
        user_agent=os.environ['USER_AGENT'],
        username=os.environ['USERNAME'],
    )

    if not os.path.exists(os.path.join(BASE_DIR, 'Reddit.2')):
        os.makedirs(os.path.join(BASE_DIR, 'Reddit.2'))

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

        # Calculate growth rates
        current_upvotes = submission.ups
        current_comments = submission.num_comments
        
        # Get previous values for growth rate calculation
        previous_upvote_count = previous_upvotes.get(submission.id, current_upvotes)
        previous_comment_count = previous_comments.get(submission.id, current_comments)
        
        # Calculate growth rates
        upvotes_growth_rate = current_upvotes - previous_upvote_count
        comments_growth_rate = current_comments - previous_comment_count

        # Store current values for future reference
        previous_upvotes[submission.id] = current_upvotes
        previous_comments[submission.id] = current_comments

        post_data = {
            'unique_id': submission.id,
            'post_heading': submission.title,
            'link_url': submission.url,
            'post_url': f'https://www.reddit.com{submission.permalink}',  
            'publish_time': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d'),  # Keep date
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
    log_filename = os.path.join(BASE_DIR, f'Reddit.2/reddit_f1_log_{datetime.now().strftime("%Y%m%d_%H%M")}.csv')
    reddit_df.to_csv(log_filename, index=False)

    # Save the updated upvotes and comments data for the next run
    save_current_data('Reddit.2/previous_upvotes.json', previous_upvotes)
    save_current_data('Reddit.2/previous_comments.json', previous_comments)

if __name__ == "__main__":
    main()
