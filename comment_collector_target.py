# Use pushshift to get posts, use collected post to go to reddit and get comments

import math
import json
import requests
import itertools
import numpy as np
import time
from datetime import datetime, timedelta
import sqlite3

import praw
config = { # reddit creditials
    "username" : "",
    "password" : "",
    "client_id" : "",
    "client_secret" : "",
    "user_agent" : ""
}
reddit = praw.Reddit(client_id = config['client_id'], \
                     client_secret = config['client_secret'], \
                     user_agent = config['user_agent'], \
                     username = config['username'], \
                     password = config['password'])


def make_request(uri, max_retries = 5):
    def fire_away(uri):
        response = requests.get(uri)
        assert response.status_code == 200
        return json.loads(response.content)
    current_tries = 1
    while current_tries < max_retries:
        try:
            time.sleep(5)
            response = fire_away(uri)
            return response
        except:
            print("sleeping long...")
            time.sleep(120)
            current_tries += 1
    return fire_away(uri)


def pull_posts_for(subreddit, start_at, end_at):

    def map_posts(posts):
        return list(map(lambda post: { #add the post data that you want here
            'id': post['id'],
            'created_utc': post['created_utc'],
            'prefix': 't4_',
            'title': post['title']
        }, posts))

    SIZE = 500
    URI_TEMPLATE = r'https://api.pushshift.io/reddit/search/submission?subreddit={}&after={}&before={}&size={}'

    post_collections = map_posts( \
        make_request( \
            URI_TEMPLATE.format( \
                subreddit, start_at, end_at, SIZE))['data'])
    n = len(post_collections)
    while n == SIZE:
        last = post_collections[-1]
        new_start_at = last['created_utc'] - (10)

        more_posts = map_posts( \
            make_request( \
                URI_TEMPLATE.format( \
                    subreddit, new_start_at, end_at, SIZE))['data'])

        n = len(more_posts)
        post_collections.extend(more_posts)
    return post_collections

def give_me_intervals(start_at, number_of_days_per_interval = 3):

    end_at = math.ceil(datetime.utcnow().timestamp())

    ## 1 day = 86400,
    period = (86400 * number_of_days_per_interval)
    end = start_at + period
    yield (int(start_at), int(end))
    padding = 1
    while end <= end_at:
        start_at = end + padding
        end = (start_at - padding) + period
        yield int(start_at), int(end)


def getcomments(subreddit, submission_id):
    TIMEOUT_AFTER_COMMENT_IN_SECS = .350
    cmt_table = []
    submission = reddit.submission(id=submission_id)
    submission_title = submission.title
    print(submission_title, "Subreddit:", subreddit)
    submission.comments.replace_more(limit=None)
    for comment in submission.comments.list():
        try:
            author_name = comment.author.name
        except:
            author_name = "[deleted]"
        cmt_row = [submission.id, submission_title, subreddit, comment.id, \
        author_name, comment.body, comment.created_utc]
        cmt_table.append(cmt_row)

        if TIMEOUT_AFTER_COMMENT_IN_SECS > 0:
            time.sleep(TIMEOUT_AFTER_COMMENT_IN_SECS)
    return cmt_table

def getsubs(subfile):
    subs = []
    with open(subfile) as f:
        for line in f:
            subs.append(line.strip())
    return subs

def writeout(cmt_table):
    commentdb = "comments.db"
    conn = sqlite3.connect(commentdb)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS comments_gamethreads (submission_id text, submission_title text, \
    subreddit text, comment_id text, comment_author text, comment_body text, comment_created int)''')
    for cmt_row in cmt_table:
        c.execute('''INSERT INTO comments_gamethreads VALUES(?,?,?,?,?,?,?)''', tuple(cmt_row))
    conn.commit()

## Main <-------------------
submission_id = ('NYGiants', "j7e6a6") # give subreddit and specific submission id to collect comments from
print("Getting commets for specific submission id:", submission_id)
cmt_table = getcomments(submission_id[0], submission_id[1])
writeout(cmt_table)
