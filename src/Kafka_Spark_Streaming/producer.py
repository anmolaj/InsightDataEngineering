from kafka import KafkaProducer
from kafka.errors import KafkaError
import tweepy


accessToken="XXXX"
accessTokenSecret="XXXX"
consumerKey="XXXX"
consumerSecret="XXXX"

auth=tweepy.OAuthHandler(consumerKey,consumerSecret)

topic="tweet-topic"
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

class TweetListener(tweepy.StreamListener):
	"""
	Perform Appropriate Action on Stream
	input : StreamListener 
	return : Approrpiate Error or sending tweets to consumer
	"""
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
	Send tweets as a Stream for PySpark Consumer
	Output: Tweets in English
	"""
	
	auth=tweepy.OAuthHandler(consumerKey,consumerSecret)
	auth.set_access_token(accessToken,accessTokenSecret)

	stream=tweepy.streaming.Stream(auth,TweetListener())
	stream.sample(languages=['en'])


