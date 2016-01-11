#create twitter stream listner
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import time
import json
import sys
import csv
from py2neo import Graph
from neo4j import tweet_to_neo4j

#create keys, ideally these keys should be stored remotely to avoid being made public on GitHub.
consumer_key = 'hE2xXOptfraLvpgpmxcZMC4N5'
consumer_secret = 'rTM7K7AKxiJekWQkwZmFoze03L22t5XFWW5Mr3nbW29XuMEVJZ'
access_token_key = '75924798-4PeAF7UK7sSj7GFdg7gyGoRpNnw3l5eq69yKUzlu0'
access_token_secret = 'SkZZSbUNawzAwsl2bthqbtuiNBG4X67TSiYEaE7phNmaW'

#authoize th twitter stream
auth = OAuthHandler(consumer_key, consumer_secret)
auth.secure = True
auth.set_access_token(access_token_key, access_token_secret)

#neo4j graph
graph = Graph("http://neo4j:Paran0id!@127.0.0.1:7474/db/data/")

class TwitterListener(StreamListener):
    """ A Handlers which sends tweets received by the string to the current RDD
    """
    def __init__(self):
#         self.bucket = to_rdd
        self.start_time = time.time()
#         self.queue = rddQueue
#         self.ssc = ssc
    # def on_connect(self):
    #     counter = 1
    #     for i in range(0,2):
    #         self.filename = "min%s.csv" % str(i)
    #         time.sleep(10)
    #         counter += 1

    def on_connect(self):
        time.sleep(1)

    def on_data(self, data):

        try:
            data = ParseTweet(data)
            tweet_to_neo4j(data,graph)
            print str(data)
#
            tweet_file = open('tweet_file.csv','a')
            tweet_file.write(data)
            tweet_file.write('\n')
            tweet_file.close()
            return True
        except BaseException, e:
            print 'Failed on_data because ', str(e)
            time.sleep(2)



    def on_error(self, status):
        print(status)



def ParseTweet(tweet):
    """From every tweet, returns only data relevant to Yewno task
    """

    temp = json.loads(tweet)

    tweet_id = temp['id']
    user_id = temp['user']['id']
    user_name = temp['user']['name']
    tweet_text = temp['text']
    hash_tags = temp['entities']['hashtags']
    symbols = temp['entities']['symbols']

    #if the tweet is retweeting something, also grab the id of the retweeted tweet.
    if temp.has_key('retweeted_status'):
        RT_id = temp['retweeted_status']['id']
    else:
        RT_id = 'null'

    return [tweet_id,
            user_id,
            user_name,
            tweet_text,
            hash_tags,
           symbols,
            RT_id]

if __name__ == '__main__':

    l = TwitterListener()

    stream = Stream(auth, l)
    stream.filter(track=['basketball'])

