from kafka import KafkaProducer
from kafka.errors import KafkaError
import tweepy

accessToken="760434835076841472-CPywqvwOqLfPWuPvaPGgVk0yncWos2Z"
accessTokenSecret="zjCndaS5EfozdIpxs56GRMbBmFSlCFkxZxyzgZqsmHWOQ"
consumerKey="XBFHbTTD6RJM4w6UpLnLrUGUv"
consumerSecret="oXrjle67jCXjJBR8S8LFiiwAocPUASOFFOZNR8ctKDGEVBC5wW"

auth=tweepy.OAuthHandler(consumerKey,consumerSecret)

topic="tweet-topic2"
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

class TweetListener(tweepy.StreamListener):
    def on_status(self,status):
    	producer.send(topic,key=str(status.created_at),value=status.text.encode('utf-8'))
    
        
        return True
    def on_error(self,statusCode):
        print "Error Code: %s"%statusCode
        return True
    
    def on_timeout(self):
        print "TimeOut"
        return True

if __name__ == "__main__":
	"""
	Motivation: Send tweets as a Stream for PySpark Consumer
	Output: Tweets
	"""
	
	

	auth=tweepy.OAuthHandler(consumerKey,consumerSecret)
	auth.set_access_token(accessToken,accessTokenSecret)

	stream=tweepy.streaming.Stream(auth,TweetListener())
	stream.sample(languages=['en'])

	# Asynchronous by default
	# future = producer.send('tweet-topic2', b'raw_bytes')

	# # Block for 'synchronous' sends
	# try:
	#     record_metadata = future.get(timeout=10)
	# except KafkaError:
	#     # Decide what to do if produce request failed...
	#     log.exception()
	#     pass

	# # Successful result returns assigned partition and offset
	# print (record_metadata.topic)
	# print (record_metadata.partition)
	# print (record_metadata.offset)

	# # produce keyed messages to enable hashed partitioning
	# producer.send('tweet-topic2', "Trial message 1")
	# producer.send('tweet-topic2', "Trial message 2")
	

