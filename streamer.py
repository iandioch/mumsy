import tweepy
from tweepy import OAuthHandler
import json
#import sqlite3
import sys
from datetime import datetime 
import bson
import gearman

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


"""database_connection = sqlite3.connect('irish_tweets.db')
db_cursor = database_connection.cursor()
db_cursor.execute('CREATE TABLE tweets (user text, place text, contents text, json text)')
"""
num_tweets = 0
tweets_this_min = 0

last_check_time = datetime.now().timestamp()

class IrelandListener(tweepy.StreamListener):
    def on_status(self, status):
        global num_tweets
        global last_check_time
        global tweets_this_min
        """data = status._json
        if data["place"]["country_code"] == "IE" or "Ireland" in data["place"]["full_name"]:
            num_tweets += 1
            tweet_data = (str(data['user']['screen_name']), str(data['place']), str(data['text']), str(data))
            print(tweet_data)
            db_cursor.execute('INSERT INTO tweets VALUES (?,?,?,?)', tweet_data)
            database_connection.commit()
            #print(json.dumps(data, indent=2))
        else:
            print("Not in Ireland:")
            print(data["place"])
        if num_tweets >= 100000 or datetime.now().hour >= 14:
            database_connection.close()
            sys.exit()"""
        bson_data = bson.BSON.encode(status._json)
        gearman.submit_job('db-add', 'tweets', str(datetime.day) + "/" + str(datetime.month) + "/" + str(datetime.year), bson_data)

        num_tweets += 1
        tweets_this_min += 1
        if datetime.now().timestamp() - last_check_time >= 60:
            print(str(tweets_this_min) + " tweets this min, total = " + str(num_tweets))
            tweets_this_min = 0
            last_check_time = datetime.now().timestamp()

    def on_error(self, status_code):
        print('Error with status code: ' + str(status_code))

    def on_timeout(self):
        print('Timeout')

ireland_listener = IrelandListener()
ireland_stream = tweepy.Stream(auth=api.auth, listener=ireland_listener)
#ireland_stream.filter(locations=[11.0,56.0,7.0,51.0])
#ireland_stream.filter(languages='en', locations=[-6.38,49.87,1.77,55.81])
#ireland_stream.filter(languages=['en'], track=['mam,mum,mom'], locations=[11.0,56.0,7.0,51.0])
#ireland_stream.filter(locations=[-7,53,-5.68,53.64]) # dublin
ireland_stream.filter(locations=[-12.2277,51.2705,-4,55.142])
