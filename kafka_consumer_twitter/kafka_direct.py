# -*- coding: utf-8 -*-
import sys
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
	#topic = 'CUT'
	brokers = 'node1.shinigami.com:9092,node3.shinigami.com:9092'
	appName = "CUT"
	conf = SparkConf().setAppName(appName)
	sc = SparkContext(conf=conf)
	ssc = StreamingContext(sc, 2)
	#brokers, topic = sys.argv[1:]
	kvs = KafkaUtils.createDirectStream(ssc,['CUT'],{'bootstrap.servers': brokers})
	lines = kvs.map(lambda x: x[1])
    	counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
	counts.pprint()
	print "KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK"
    	ssc.start()
    	ssc.awaitTermination()
