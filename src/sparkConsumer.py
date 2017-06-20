from __future__ import print_function

# import pickle
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# import nltk


class MainTweets():


	def __init__(self):
		import re
		import string

		import nltk
		import pandas as pd
		from sklearn.naive_bayes import MultinomialNB
		import re
		import string
		import glob
		from collections import Counter
		from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer


		categories=['business','entertainment','politics','sport','tech']
		file_name="bbc2/%s/*.txt"
		news_df=self.get_data(file_name,categories)
		

		processed_df=self.process_all(news_df)
		rare_words=self.get_rare_words(processed_df)

		self.count_vectorizer=self.create_features(processed_df,rare_words)
		self.tweet_classifier,self.tfidf_transformer=self.tweet_classifier_model(self.count_vectorizer,processed_df)

	def get_data(self,file_name,categories):

		import re
		import string

		import nltk
		import pandas as pd
		from sklearn.naive_bayes import MultinomialNB
		import re
		import string
		import glob
		from collections import Counter
		from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer

		training_set=[]
		for cat in categories:
			for each_doc in glob.glob(file_name%cat):
				with open(each_doc) as fi:
					training_set.append((cat,fi.read()))

		labels=['category','text']
		news_df=pd.DataFrame(training_set,columns=labels)
		return news_df

	def process(self,text):

		import re
		import string

		import nltk
		import pandas as pd
		import re
		import string
		import glob
		from collections import Counter
	


		lemmatizer=nltk.stem.wordnet.WordNetLemmatizer()
		text=text.lower()
		text=text.replace('\'s','')
		text=text.replace('\'','')
		text = re.sub("\d+", "", text)
		lemmatizedtext=''


		for punct in string.punctuation:
			lemmatizedtext=lemmatizedtext.replace(punct,' ')  

		tokens=[]
		for punct in string.punctuation:
			text=text.replace(punct,' ')       

		for token in nltk.word_tokenize(text):
			try:
				tokens.append(lemmatizer.lemmatize(token).encode())
			except:
				continue

		return tokens
		pass

	def process_all(self,df):
		import re
		import string

		import nltk
		import pandas as pd
		from sklearn.naive_bayes import MultinomialNB
		import re
		import string
		import glob
		from collections import Counter
		from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer

		lemmatizer=nltk.stem.wordnet.WordNetLemmatizer()
		for ix,article in df["text"].iteritems():
		    df['text'][ix]=self.process(article)
		return df
		pass

	def get_rare_words(self,processed_articles):
		import re
		import string

		import nltk
		import pandas as pd
		from sklearn.naive_bayes import MultinomialNB
		import re
		import string
		import glob
		from collections import Counter
		from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer

		c=Counter()
		all_articles=[]
		for articles in processed_articles["text"]:
		    all_articles.extend(articles)
		c.update(all_articles)
		rare_words=[(k) for (k,v) in sorted(c.items()) if v==1 ]
		return rare_words
		pass

	def create_features(self,processed_articles,rare_words):
		import re
		import string

		import nltk
		import pandas as pd
		from sklearn.naive_bayes import MultinomialNB
		import re
		import string
		import glob
		from collections import Counter
		from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
	    
		stopwords=nltk.corpus.stopwords.words('english')

		data=[(" ").join(t) for t in processed_articles["text"]]
		count_vectorizer = CountVectorizer(stop_words=(rare_words+[x.encode() for x in stopwords])).fit(data)
		return count_vectorizer
		pass

	def tweet_classifier_model(self,count_vectorizer,processed_df):
		import re
		import string

		import nltk
		import pandas as pd
		from sklearn.naive_bayes import MultinomialNB
		import re
		import string
		import glob
		from collections import Counter
		from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer

		transformed_count_vectorizer=count_vectorizer.transform([" ".join(list_text) for list_text in processed_df["text"]])
		tfidf_transformer = TfidfTransformer().fit(transformed_count_vectorizer)
		articles_tfidf = tfidf_transformer.transform(transformed_count_vectorizer)
		tweet_classifier=MultinomialNB().fit(articles_tfidf, processed_df['category'])

		return (tweet_classifier,tfidf_transformer)
		pass
        
	def predict(self,tweet):

		processed_tweet=self.process(tweet)
		tweet_count_vectorizer=self.count_vectorizer.transform([" ".join(processed_tweet)])
		processed_tweet_final=self.tfidf_transformer.transform(tweet_count_vectorizer)

		return self.tweet_classifier.predict(processed_tweet_final)[0]
		pass

# def process(text, lemmatizer=nltk.stem.wordnet.WordNetLemmatizer()):
#     """ Normalizes case and handles punctuation
#     Inputs:
#         text: str: raw text
#         lemmatizer: an instance of a class implementing the lemmatize() method
#                     (the default argument is of type nltk.stem.wordnet.WordNetLemmatizer)
#     Outputs:
#         list(str): tokenized text
#     """
#     text=text.lower()
#     text=text.replace('\'s','')
#     text=text.replace('\'','')
#     text = re.sub("\d+", "", text)
#     lemmatizedtext=''


#     for punct in string.punctuation:
#         lemmatizedtext=lemmatizedtext.replace(punct,' ')  

#     tokens=[]
#     for punct in string.punctuation:
#         text=text.replace(punct,' ')       

#     for token in nltk.word_tokenize(text):
#         try:
#             tokens.append(lemmatizer.lemmatize(token).encode())
#         except:
#             continue

#     return tokens
#     pass

# class tweetClass():
#     def __init__(self,processed_df,rare_words,count_vectorizer):

#         self.count_vectorizer=create_features(processed_df,rare_words)
#         self.tweet_classifier,self.tfidf_transformer=tweet_classifier_model(count_vectorizer,processed_df)

    
#     def predict(self,tweet):
         
#         processed_tweet=process(tweet)
#         tweet_count_vectorizer=self.count_vectorizer.transform([" ".join(processed_tweet)])
#         processed_tweet_final=self.tfidf_transformer.transform(tweet_count_vectorizer)

#         return self.tweet_classifier.predict(processed_tweet_final)[0]

if __name__ == "__main__":

	try:
		del sc
	except:
		pass
	sc = SparkContext(appName="TweetProcess")
	ssc= StreamingContext(sc,5)
	sc.setLogLevel("WARN")
	

	

	print ("Start---------------")
	tweet_model= MainTweets()

	brokers = 'ip-172-31-63-36:9092,ip-172-31-63-107:9092'
	topic="tweet-topic"

	print ("Read---------------")
	ioStream=KafkaUtils.createDirectStream(ssc,[topic],{"metadata.broker.list": brokers})
	trialTxt="RT @totallydolan: I'm happy the twins aren't v active rn. They must be spending time with their family"
	# filename = 'twitter.pkl'
	# loaded_model = pickle.load(open(filename, 'rb'))
	#lines1 =ioStream.map(lambda x:(x[0],process(x[1])))
	#lines1.pprint()
	#print (trialTxt,loaded_model.predict(trialTxt))

	# print (trialTxt,x.predict(trialTxt))
	lines =ioStream.map(lambda x:(x[1],tweet_model.predict(x[1])))
	
	lines.pprint()

	



	ssc.start()
	ssc.awaitTermination()
    


