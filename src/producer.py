from kafka import KafkaProducer
from kafka.errors import KafkaError
import tweepy


accessToken="760434835076841472-CPywqvwOqLfPWuPvaPGgVk0yncWos2Z"
accessTokenSecret="zjCndaS5EfozdIpxs56GRMbBmFSlCFkxZxyzgZqsmHWOQ"
consumerKey="XBFHbTTD6RJM4w6UpLnLrUGUv"
consumerSecret="oXrjle67jCXjJBR8S8LFiiwAocPUASOFFOZNR8ctKDGEVBC5wW"

# accessToken="XXXX"
# accessTokenSecret="XXXX"
# consumerKey="XXXX"
# consumerSecret="XXXX"

auth=tweepy.OAuthHandler(consumerKey,consumerSecret)

topic="tweet-topic"
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

class TweetListener(tweepy.StreamListener):
    def on_status(self,status):
    	producer.send(topic,key=str(status.created_at),value=status.text.encode('utf-8'))
    	print ("Sent: ",str(status.created_at))
        
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


