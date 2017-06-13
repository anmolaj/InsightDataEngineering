from kafka import KafkaProducer
from kafka.errors import KafkaError

def produce():
	"""
	Motivation: Send tweets as a Stream for PySpark Consumer
	Output: Tweets
	"""
	producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

	# Asynchronous by default
	future = producer.send('my-topic', b'raw_bytes')

	# Block for 'synchronous' sends
	try:
	    record_metadata = future.get(timeout=10)
	except KafkaError:
	    # Decide what to do if produce request failed...
	    log.exception()
	    pass

	# Successful result returns assigned partition and offset
	print (record_metadata.topic)
	print (record_metadata.partition)
	print (record_metadata.offset)

	# produce keyed messages to enable hashed partitioning
	producer.send('my-topic', key=b'foo', value=b'bar')
	producer.send('my-topic', b"Trial message")

if __name__ == "__main__":
	produce()

