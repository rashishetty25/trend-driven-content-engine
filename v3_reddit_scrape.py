import praw
import pandas as pd
from datetime import datetime, timedelta
import os
import json

# Function to load previous data from JSON file
def load_previous_data(filename):
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            return json.load(file)
    return {}

# Function to save current data to JSON file
def save_current_data(filename, data):
    with open(filename, 'w') as file:
        json.dump(data, file)

# Function to append a new value to a key in the data dictionary
def append_value(key, new_value, data):
    if key not in data:
        data[key] = []  
    elif not isinstance(data[key], list):
        data[key] = [data[key]]  
    data[key].append(new_value)
    
    if len(data[key]) > 24:
        data[key].pop(0)  # Keep only the last 24 records

def main():
    # Initialize Reddit API client
    reddit = praw.Reddit(
        client_id=os.environ['CLIENT_ID'],
        client_secret=os.environ['CLIENT_SECRET'],
        password=os.environ['PASSWORD'],
        user_agent=os.environ['USER_AGENT'],
        username=os.environ['USERNAME'],
    )

    # Load previous data
    previous_upvotes = load_previous_data('Reddit.3/previous_upvotes.json')
    previous_comments = load_previous_data('Reddit.3/previous_comments.json')
    previous_downvotes = load_previous_data('Reddit.3/previous_downvotes.json')
    previous_upvote_ratios = load_previous_data('Reddit.3/previous_upvote_ratios.json')

    subreddit = reddit.subreddit('formula1')
    posts_data = []

    # Define excluded flairs
    excluded_flairs = {'Off-Topic', 'Misc', 'Poster', 'Photo', 'Daily Discussion', 'AMA', 'Satire', 'Automated Removal'}

    # Collect top posts from the past day
    for submission in subreddit.top(time_filter='day', limit=1000):
        flair_tag = submission.link_flair_text
        if flair_tag and flair_tag.split(':')[-1].strip() in excluded_flairs:
            continue

        # Calculate post age in days
        total_seconds = (datetime.utcnow() - datetime.utcfromtimestamp(submission.created_utc)).total_seconds()
        post_age_days = total_seconds // 86400
        post_age = f"{int(post_age_days)} days" if post_age_days > 0 else "0 days"

        # Variables for comment-based data
        total_upvotes_comments = 0
        total_downvotes_comments = 0
        
        # Collect upvotes and downvotes from top 10 comments
        try:
            submission.comments.replace_more(limit=0)
            for comment in submission.comments.list()[:10]:
                if comment.author and comment.author.name != "AutoModerator":
                    total_upvotes_comments += comment.ups
                    total_downvotes_comments += max(comment.score * -1, 0)
        except Exception as e:
            print(f"Error fetching comments for {submission.id}: {e}")

        # Collect current upvotes and comments
        current_upvotes = submission.ups
        current_comments = submission.num_comments

        # Calculate estimated downvotes
        previous_upvote_count = previous_upvotes.get(submission.id, [0])[-1]
        estimated_downvotes = max(submission.score - current_upvotes, 0)

        # Calculate growth rates and ratios
        previous_comment_count = previous_comments.get(submission.id, [0])[-1]
        current_upvote_ratio = current_upvotes / (current_upvotes + estimated_downvotes) if (current_upvotes + estimated_downvotes) > 0 else 0

        # Update history for each metric
        append_value(submission.id, current_upvotes, previous_upvotes)
        append_value(submission.id, current_comments, previous_comments)
        append_value(submission.id, estimated_downvotes, previous_downvotes)
        append_value(submission.id, current_upvote_ratio, previous_upvote_ratios)

        # Calculate growth rate for upvotes and comments
        upvotes_growth_rate = current_upvotes - previous_upvote_count
        comments_growth_rate = current_comments - previous_comment_count

        # Prepare post data
        post_data = {
            'unique_id': submission.id,
            'post_heading': submission.title,
            'link_url': submission.url,
            'post_url': f'https://www.reddit.com{submission.permalink}',
            'publish_time': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d'),
            'post_age': post_age,
            'total_upvotes_comments': total_upvotes_comments, 
            'total_downvotes_comments': total_downvotes_comments, 
            'upvotes': current_upvotes,
            'comments': current_comments,
            'downvotes': estimated_downvotes,
            'upvote_ratio': current_upvote_ratio,
            'upvotes_growth_rate': upvotes_growth_rate,
            'comments_growth_rate': comments_growth_rate,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'tag': flair_tag.split(':')[-1] if flair_tag else None
        }
        posts_data.append(post_data)

    # Convert to DataFrame and save to CSV
    reddit_df = pd.DataFrame(posts_data)
    log_filename = f'Reddit.3/reddit_f1_log_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    reddit_df.to_csv(log_filename, index=False)

    # Save updated historical data
    save_current_data('Reddit.3/previous_upvotes.json', previous_upvotes)
    save_current_data('Reddit.3/previous_comments.json', previous_comments)
    save_current_data('Reddit.3/previous_downvotes.json', previous_downvotes)
    save_current_data('Reddit.3/previous_upvote_ratios.json', previous_upvote_ratios)

if __name__ == "__main__":
    main()
