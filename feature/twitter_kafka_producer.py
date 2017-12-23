from kafka import SimpleProducer, KafkaClient,KafkaProducer
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from twitter_config import consumer_key, consumer_secret, access_token, access_token_secret

#topic = "testTopic2"
#kafka = KafkaClient('node1.shinigami.com:9092')
#producer = SimpleProducer(kafka)
#producer = KafkaProducer(bootstrap_servers=['localhost:6667'])

class WriteTopic(StreamListener):
	def __init__(self):
		self.topic = "testTopic2"
		self.producer = KafkaProducer(bootstrap_servers=['localhost:6667'])
		self.track = "the to and is in it you of for on my".split()
	
	def on_data(self,data):
		self.producer.send(topic,data.encode('utf-8'))
		print data
		return True

	def on_error(self, status):
		print status

if __name__ == '__main__':
	print 'running the twitter-stream python'
	w=WriteTopic()
	print "write topic"
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	print "AUTH READY"
	stream = Stream(auth, w)
	print w.track
	while True:
		try:
			t = stream.filter(languages = ["en"],track = self.track)
			print t
			print "test"
		except:
			pass
