import praw
import pandas as pd
from datetime import datetime, timedelta
import os
import json

def load_previous_data(filename):
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            return json.load(file)
    return {}

def save_current_data(filename, data):
    with open(filename, 'w') as file:
        json.dump(data, file)

def append_value(key, new_value, data):
    if key not in data:
        data[key] = []  
    elif not isinstance(data[key], list):
        data[key] = [data[key]]  
    data[key].append(new_value)
    
    if len(data[key]) > 24:
        data[key].pop(0)  

def main():
    reddit = praw.Reddit(
        client_id=os.environ['CLIENT_ID'],
        client_secret=os.environ['CLIENT_SECRET'],
        password=os.environ['PASSWORD'],
        user_agent=os.environ['USER_AGENT'],
        username=os.environ['USERNAME'],
    )

    previous_upvotes = load_previous_data('Reddit.3/previous_upvotes.json')
    previous_comments = load_previous_data('Reddit.3/previous_comments.json')
    previous_upvote_ratios = load_previous_data('Reddit.3/previous_upvote_ratios.json')
    previous_score = load_previous_data('Reddit.3/previous_score.json')

    subreddit = reddit.subreddit('formula1')
    posts_data = []

    excluded_flairs = {'Off-Topic', 'Misc', 'Poster', 'Photo', 'Daily Discussion', 'AMA', 'Satire', 'Automated Removal'}

    for submission in subreddit.hot(limit=1000):
        flair_tag = submission.link_flair_text
        if flair_tag and flair_tag.split(':')[-1].strip() in excluded_flairs:
            continue

        total_upvotes_comments = 0
        total_score_comments = 0
        
        try:
            submission.comments.replace_more(limit=0)  
            for comment in submission.comments.list():
                if comment.author and comment.author.name != "AutoModerator":
                    total_upvotes_comments += comment.ups
                    total_score_comments += comment.score  
        except Exception as e:
            print(f"Error fetching comments for {submission.id}: {e}")


        current_upvotes = submission.ups
        current_comments = submission.num_comments
        current_upvote_ratio = submission.upvote_ratio
        current_score = submission.score
        
        previous_upvote_count = previous_upvotes.get(submission.id, [0])[-1]
        upvotes_growth_rate = current_upvotes - previous_upvote_count
        
        previous_comment_count = previous_comments.get(submission.id, [0])[-1]
        comments_growth_rate = current_comments - previous_comment_count
        
        previous_upvote_ratio_count = previous_upvote_ratios.get(submission.id, [0])[-1]
        upvote_ratio_growth_rate = current_upvote_ratio - previous_upvote_ratio_count
        
        previous_score_count = previous_score.get(submission.id, [0])[-1]
        score_growth_rate = current_score - previous_score_count

        append_value(submission.id, current_upvotes, previous_upvotes)
        append_value(submission.id, current_comments, previous_comments)
        append_value(submission.id, current_upvote_ratio, previous_upvote_ratios)
        append_value(submission.id, current_score, previous_score)

        post_data = {
            'unique_id': submission.id,
            'post_heading': submission.title,
            'post_url': f'https://www.reddit.com{submission.permalink}',
            'publish_time': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d'),
            'tag': flair_tag.split(':')[-1] if flair_tag else None,
            'post_content': submission.selftext,
            'top_comments': [comment.body for comment in submission.comments.list()[:10] if comment.author and comment.author.name != "AutoModerator"],
            'upvotes': current_upvotes,
            'comments': current_comments,
            'upvote_ratio': current_upvote_ratio,
            'score': current_score,
            'total_upvotes_comments': total_upvotes_comments, 
            'total_score_comments': total_score_comments, 
            'upvotes_growth_rate': upvotes_growth_rate,
            'comments_growth_rate': comments_growth_rate,
            'upvote_ratio_growth_rate': upvote_ratio_growth_rate,
            'score_growth_rate': score_growth_rate            
        }
        posts_data.append(post_data)
        
    reddit_df = pd.DataFrame(posts_data)
    log_filename = f'Reddit.3/reddit_f1_log_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    reddit_df.to_csv(log_filename, index=False)

    save_current_data('Reddit.3/previous_upvotes.json', previous_upvotes)
    save_current_data('Reddit.3/previous_comments.json', previous_comments)
    save_current_data('Reddit.3/previous_upvote_ratios.json', previous_upvote_ratios)
    save_current_data('Reddit.3/previous_score.json', previous_score)

if __name__ == "__main__":
    main()
