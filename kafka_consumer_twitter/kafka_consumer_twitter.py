from confluent_kafka import Consumer, KafkaError

running = True
topic = b'CUT'
config = {'bootstrap.servers': 'localhost:9092,node3.shinigami.com:9092','group.id':'group'}
c = Consumer(config)
c.subscribe([topic])
while running:
	msg = c.poll()
	if not msg.error():
		data = msg.value().decode('utf-8')
		print "DATA is", data
	elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                self.running = False
c.close()
