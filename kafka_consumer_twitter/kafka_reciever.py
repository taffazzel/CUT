from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


if __name__=="__main__":
	appName = "Reciever"
	conf = SparkConf(appName)
	sc = SparkContext(conf=conf)
	ssc = StreamingContext(sc,10)
	kvs = KafkaUtils.createStream(ssc,'localhost:9092,node3.shinigami.com',"ttt",{'CUT':1})
	lines = kvs.map(lambda x: x[1])
    	counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
	counts.pprint()
	print "MESSAGE"
	#lines.pprint()
    	ssc.start()
   	ssc.awaitTermination()
