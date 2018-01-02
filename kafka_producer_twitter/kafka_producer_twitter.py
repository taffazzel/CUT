import json
import confluent_kafka 
from kafka import KafkaProducer, KafkaClient

import tweepy
from tweepy import OAuthHandler, Stream, API
from tweepy.streaming import StreamListener
import twitter_config

consumer_key = twitter_config.consumer_key
consumer_secret = twitter_config.consumer_secret
access_token = twitter_config.access_token
access_token_secret = twitter_config.access_token_secret

bootstrap_servers = 'node1.shinigami.com:6667'
topic = b'CUT'
conf = {'bootstrap.servers': bootstrap_servers}
producer = confluent_kafka.Producer(**conf)


class TweetStreamListener(StreamListener):
    #def __init__(self,api):
        #print ( "In init")
        #self.api = api
        #super(StreamListener, self).__init__()
        #conf = {'bootstrap.servers': bootstrap_servers}
        #producer = confluent_kafka.Producer(**conf)
        #client = KafkaClient("localhost:6667")
        #self.producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'))

    def on_status(self, status):
        """
        This is called when new data arrives as live stream
        """
        print ( "In on_status")
        text = status.text.encode('utf-8')
        #text = json.loads(status)['text'].encode('utf-8')
        print ("The data : ",str(text))
        try:
            producer.produce(topic, value=text)
            print ("In the TRY")
        except Exception as e:
            print (e)
            return False
        return True

    def on_error(self, status_code):
        print ("Error received in kafka producer, ", status_code)
        return True #don't kill the stream

    def on_timeout(self):
        return True

if __name__ == '__main__':
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = API(auth)

    #stream = Stream(auth, listener=TweetStreamListener(api))
    listener = TweetStreamListener()
    stream = Stream(auth, listener)
    #stream.filter(track=['news'],languages=["en"])
    stream.sample()


