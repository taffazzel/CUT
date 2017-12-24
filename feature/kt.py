import tweepy
import threading, logging, time
from kafka.client import KafkaClient
from kafka import SimpleProducer, SimpleClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer
import string
from kafka import KafkaProducer


consumer_key = "pIhJP9BBX7QDkMrieXWOUcEv5"
consumer_secret = "YGUIPrmTGCyoO40wKCv3PboBpdwkgmTxw1iytjHfwlmjQfOPvu"
access_token = "1627712238-HlOCH9yPSyCrOsATa7QRCCvTrZrKfmESw754cny"
access_token_secret = "W6M7KUHy9RHcWXxNb8OUSPJWhJk6H3XheHea2g05YLb3B"
mytopic='testTopic2'

class StdOutListener(tweepy.StreamListener):
	
	def on_status(self,status):
		print "I am in on status"
		#print '%d,%d,%d,%s,%s' % (status.user.followers_count, status.user.friends_count,status.user.statuses_count, status.text, status.user.screen_name)
		producer = KafkaProducer(bootstrap_servers=['localhost:6667'])
	        message = status.user.screen_name
		print "MESSAGE",message
	       	message = filter(lambda x: x in string.printable, message)		
		try:
			print "Hello"
		        producer.send(mytopic,message)
				
		except Exception, e:
	        	return True        

        	return True
		
	def on_error(self, status_code):
	        print('Got an error with status code: ' + str(status_code))
        	return True
		
	def on_timeout(self):
		print "Timeout"
		return True
	

if __name__== '__main__':
	listener = StdOutListener()
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token,access_token_secret)
	stream = tweepy.Stream(auth,listener)	
	#producer = KafkaProducer(bootstrap_servers=['localhost:6667'])
	stream.sample()
