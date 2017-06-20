import nltk
import pandas as pd
from sklearn.naive_bayes import MultinomialNB
import re
import string
import glob
from collections import Counter
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer


class MainTweets():

	def __init__(self):

		categories=['business','entertainment','politics','sport','tech']
		file_name="bbc2/%s/*.txt"
		news_df=self.get_data(file_name,categories)
		

		processed_df=self.process_all(news_df)
		rare_words=self.get_rare_words(processed_df)

		self.count_vectorizer=self.create_features(processed_df,rare_words)
		self.tweet_classifier,self.tfidf_transformer=self.tweet_classifier_model(self.count_vectorizer,processed_df)

	def get_data(self,file_name,categories):

		training_set=[]
		for cat in categories:
			for each_doc in glob.glob(file_name%cat):
				with open(each_doc) as fi:
					training_set.append((cat,fi.read()))

		labels=['category','text']
		news_df=pd.DataFrame(training_set,columns=labels)
		return news_df

	def process(self,text, lemmatizer=nltk.stem.wordnet.WordNetLemmatizer()):
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

	def process_all(self,df,lemmatizer=nltk.stem.wordnet.WordNetLemmatizer()):
		for ix,article in df["text"].iteritems():
		    df['text'][ix]=self.process(article)
		return df
		pass

	def get_rare_words(self,processed_articles):
		c=Counter()
		all_articles=[]
		for articles in processed_articles["text"]:
		    all_articles.extend(articles)
		c.update(all_articles)
		rare_words=[(k) for (k,v) in sorted(c.items()) if v==1 ]
		return rare_words
		pass

	def create_features(self,processed_articles,rare_words):
	    
		stopwords=nltk.corpus.stopwords.words('english')

		data=[(" ").join(t) for t in processed_articles["text"]]
		count_vectorizer = CountVectorizer(stop_words=(rare_words+[x.encode() for x in stopwords])).fit(data)
		return count_vectorizer
		pass

	def tweet_classifier_model(self,count_vectorizer,processed_df):

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


