from datetime import datetime
import sys
import pandas as pd
import tweepy as tw
import yaml
import argparse
sys.path.append('.')
from utils import connectivity as connect
import utils.logs as logs
import utils.configs_for_code as cfg

configs_file = open(cfg.PATH_CONFIG_FILE, 'r')
hashtags_file = open(cfg.PATH_HASHTAGS_FILE, 'r')
configs = yaml.load(configs_file, Loader=yaml.FullLoader)
logger = logs.create_logger(__name__)

parser = argparse.ArgumentParser(allow_abbrev=False)
# parser.add_argument("--nb-hashtag", type=str, required=True, help="number of used hashtag")
# args = parser.parse_args()
# nb_hashtag = int(args.nb_hashtag)

with hashtags_file as f:
    lines = f.readlines()
list_hashtags = lines[0].split(',')
stream_hashtags = list_hashtags[0:10]
# hashtag = list_hashtags[nb_hashtag]

hashtag_fname = 'tmp_hashtag'
# hashtag_fcontent = hashtag+','+str(nb_hashtag)
# print(hashtag_fcontent)
# with open(hashtag_fname, "w") as f:
    # f.write(hashtag_fcontent)

api = connect.connect_to_twitter()

from twitter_api import stream_listener as stl
stream_listener = stl.StreamListener()
stream = tw.Stream(auth=api.auth, listener=stream_listener)
# print(f'Start streaming with hashtag {hashtag}.')
# stream.filter(track=[hashtag])
stream.filter(track=stream_hashtags)
