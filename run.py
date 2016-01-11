import time
import json
from ConfigParser import ConfigParser
from threading import Thread
from collections import Counter

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from py2neo import Graph
from neo4j import tweet_to_neo4j


#implement config parser to avoid showing secrets on github
config = ConfigParser()
config.read('config.ini')
consumer_key = config.get("twitter", "consumer_key")
consumer_secret = config.get("twitter", "consumer_secret")
access_token_key = config.get("twitter", "access_token_key")
access_token_secret = config.get("twitter", "access_token_secret")

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
      self.hashtag_bucket = []


    # def on_connect(self):
        # self.start_time = time.time()
        # time.sleep(1)

    def on_data(self, data):

        try:

            #parse only the data we need from the tweet
            data = ParseTweet(data)
            # print data
            #write the tweet a neo4j database for later analysis
            tweet_to_neo4j(data,graph)
            #append hashtag to hashtag counter list
            hashtags = data[4]
            # add hashtags to big list
            self.hashtag_bucket = self.hashtag_bucket + hashtags
            #print running total of hashtags




            #write counter() object to a new file every 5 minutes, reset hashtag bucket

            # file_suffix += 1
            # self.hashtag_bucket = []
            # else:
            #     return True



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

    hash_tags = []
    for i in temp['entities']['hashtags']:
        hash_tags.append(i['text'])

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

def write_counter(counter,file_suffix):
    """writes a Counter() object to a file as a list of tuples"""
    filename = 'counter%s.txt' % str(file_suffix)
    for i in counter:
                file = open(filename,'a')
                file.write(str(i))
                file.write('\n')
                file.close()

class BackgroundTimer(Thread):
   def run(self):
      self.file_suffix = 1
      while 1:
          time.sleep(300)

          x = Counter(listener.hashtag_bucket).most_common(25)
          write_counter(x,self.file_suffix)
          print "writing counter"
          self.file_suffix += 1
          listener.hashtag_bucket = []




if __name__ == '__main__':
    #create listener
    listener = TwitterListener()
    #start background timer
    timer = BackgroundTimer()
    timer.start()
    #start stream
    stream = Stream(auth, listener)
    stream.filter(track=['basketball'])

