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
	#p = parsed.filter(lambda s:len(s)>2).map(lambda tweet: tweet['text'])
	parsed.filter(lambda s:len(s)>2).map(lambda tweet: tweet['text'].encode('utf-8')).pprint()
	#p.pprint()
	#parsed.pprint()
	parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()
	ssc.start()  
	ssc.awaitTermination() 
