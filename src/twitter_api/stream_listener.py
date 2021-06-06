from datetime import datetime
from textblob_de import TextBlobDE as TextBlob
from textblob import TextBlob as TextBlobEn
import tweepy as tw
import sys
import os
import yaml

sys.path.append(os.getcwd())
import utils.logs as logs
import utils.configs_for_code as cfg
from utils import connectivity as connect

configs_file = open(cfg.PATH_CONFIG_FILE, 'r')
configs = yaml.load(configs_file, Loader=yaml.FullLoader)
logger = logs.create_logger(__name__)

class StreamListener(tw.StreamListener):
    print('init class StreamListener')
    conn, cursor = connect.connect_to_azure_sql_db()
    # hashtag_fname = 'tmp_hashtag'
    # with open(hashtag_fname) as f:
    #     lines = f.readlines()
    # lines_vec = lines[0].split(',')
    # print(lines,lines_vec)
    # hashtag = lines_vec[0]
    # hashtag = hashtag.replace("'", "")
    # nb_hashtag = lines_vec[1]
    hashtags_file = open(cfg.PATH_HASHTAGS_FILE, 'r')
    with hashtags_file as f:
        lines = f.readlines()
    list_hashtags = lines[0].split(',')
    stream_hashtags = list_hashtags[0:10]

    def on_status(self, status, conn=conn, cursor=cursor, stream_hashtags=stream_hashtags):
        try:
            if status.retweeted_status:
                return
        except:
            print("This is not a retweet:")
        
        # Get text from tweet
        tweet_text = status.text
        if status.truncated == True:
            print("This tweet was shortened.")
            tweet_text = status.extended_tweet['full_text']
        tweet_text = tweet_text.replace("'", "")

        print(datetime.now(), tweet_text)
        
        target = '[sonntagsfrage].[hate_twitter_tweets_raw]'
        
        blob = TextBlob(tweet_text)
        blob_en = TextBlobEn(tweet_text)
        polarity_de = blob.sentiment[0]
        polarity_en = blob_en.sentiment[0]
        subjectivity_de = blob.sentiment[1]
        subjectivity_en = blob_en.sentiment[1]

        followers = status.user.followers_count
        fav = status.favorite_count
        
        for idx, tag in enumerate(stream_hashtags):
            if tag in tweet_text:
                list_values = [str(idx), 
                            "'"+tag+"'", 
                            "'"+tweet_text+"'", 
                            str(fav), 
                            str(followers), 
                            str(polarity_de), 
                            str(polarity_en), 
                            str(subjectivity_de), 
                            str(subjectivity_en)]
                connect.send_tweet_to_sql_db(conn, cursor, target, list_values)

    def on_error(self, status_code):
        if status_code == 420:
            return False