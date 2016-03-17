import tweepy
from tweepy import OAuthHandler
import json
import sys
from datetime import datetime 
import bson
from pymongo import MongoClient

# read oauth_data from file laid out as follows:
# consumer key
# consumer secret
# access token
# access secret
oauth_data = [s.strip() for s in open('twitter_auth.txt').readlines()]
print(oauth_data)

auth = OAuthHandler(oauth_data[0], oauth_data[1])

auth.set_access_token(oauth_data[2], oauth_data[3])

api = tweepy.API(auth)

db_client = MongoClient('localhost', 27017)
collection = db_client['tweets']['irish-ge']

num_tweets = 0
tweets_this_min = 0

last_check_time = datetime.now().timestamp()

print(last_check_time)

class IrelandListener(tweepy.StreamListener):
    def on_status(self, status):
        global num_tweets
        global last_check_time
        global tweets_this_min
        if status is None:
            print("\nstatus is None, skipping")
            return
        if "Ireland" not in status._json["place"]["country"] and status._json["place"]["country_code"] is not "IE":
            #print("\nTweet not in Ireland ('" + str(status._json["place"]["full_name"]) + ": " +str(status._json["place"]["country"]) + "'), skipping")
            print('X', end='', flush=True)
            return
        print('.', end='', flush=True)
        collection.insert_one(status._json)
        bson_data = bson.BSON.encode(status._json)

        num_tweets += 1
        tweets_this_min += 1
        if datetime.now().timestamp() - last_check_time >= 60:
            print("\n" + str(tweets_this_min) + " Irish tweets this min, total = " + str(num_tweets))
            tweets_this_min = 0
            last_check_time = datetime.now().timestamp()

    def on_error(self, status_code):
        print('Error with status code: ' + str(status_code))

    def on_timeout(self):
        print('Timeout')

ireland_listener = IrelandListener()
ireland_stream = tweepy.streaming.Stream(auth=api.auth, listener=ireland_listener)
ireland_stream.filter(locations=[-12.2277,51.2705,-4,55.142]) # the bounding box for the island of Ireland
