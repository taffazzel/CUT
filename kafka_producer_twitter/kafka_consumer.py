#pyspark --master yarn --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1
# -*- coding: utf-8 -*-

#    Spark
from pyspark import SparkContext  
#    Spark Streaming
from pyspark.streaming import StreamingContext  
#    Kafka
from pyspark.streaming.kafka import KafkaUtils  
#    json parsing
import json  
from pyspark.conf import SparkConf
#from elasticsearch import Elasticsearch

if __name__=='__main__':
	topic = b'CUT'
	brokers = 'node1.shinigami.com:6667'
	conf = SparkConf().setAppName("CUT")
	sc = SparkContext(conf = conf)
	ssc = StreamingContext(sc, 10)  
	directKafkaStream = KafkaUtils.createDirectStream(ssc, ['CUT'], {"metadata.broker.list": brokers})
	parsed = directKafkaStream.map(lambda v:json.loads(v[1]))
	#wcount = parsed.flatMap(lambda line: line.split(" ")).map(lambda w:(w,1)).reduceByKey(lambda a,b:a+b)
	#parsed.map(lambda tweet: tweet.keys()).pprint()
	#parsed.filter(lambda s:len(s)>2).map(lambda tweet: tweet).pprint()
	
	text = parsed.filter(lambda s:len(s)>2).map(lambda tweet: tweet['text'].encode('utf-8'))
	user = parsed.filter(lambda s:len(s)>2).map(lambda tweet: tweet['user']['screen_name'])
	text.pprint()
	es_conf = {"es.nodes" : "elk1.shinigami.com","es.port" : "9200","es.nodes.client.only" : "true","es.resource" : "cut/type"}
	#text.flatMap(lambda s: s.split(" ")).map(lambda w:(w,1)).reduceByKey(lambda x,y:x+y).pprint()
	user.pprint()	
	parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()
	ssc.start()  
	ssc.awaitTermination() 
