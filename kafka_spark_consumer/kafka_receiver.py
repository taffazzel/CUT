from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


if __name__== "__main__":
	broker = 'node1.shinigami.com:9092,node3.shinigami.com:9092'
	topic = 'CUT'
	appName = "KafkaReceiver"
	conf = SparkConf().setAppName(appName)
	sc = SparkContext(conf = conf)
	ssc = StreamingContext(sc,2)
	raw_message = KafkaUtils.createStream(ssc,broker,"ttt",{'CUT':1})
	#raw_message = KafkaUtils.createDirectStream(ssc,[topic],{"metadata.broker.list":broker})
	lines = raw_message.map(lambda x: x[1])	
	print "****************************************************************************"
	counts = lines.flatMap(lambda s:s.split(" ")).map(lambda w:(w,1)).reduceByKey(lambda x,y: x+y)
	counts.pprint()
	ssc.start()
	ssc.awaitTermination()
