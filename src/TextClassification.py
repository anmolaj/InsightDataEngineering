class ClassifyText():
	"""
	This Class is used to obtain Text and perform training and prediction using Multinomial Naive Bayes Classification 
	"""
	global re
	global string
	global nltk
	global pd
	global MultinomialNB
	global glob
	global Counter
	global CountVectorizer
	global TfidfTransformer

	import re
	import string
	import nltk
	import pandas as pd
	from sklearn.naive_bayes import MultinomialNB
	import glob
	from collections import Counter
	from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer


	def __init__(self):
		"""
		Initiliasing Parametes for Classification
		"""

		categories=['business','entertainment','politics','sport','tech']
		#file_name="s3n://classification-algo/bbc2/%s/*.txt"
		file_name="bbc2/%s/*.txt"
		news_df=self.get_data(file_name,categories)
		

		processed_df=self.process_all(news_df)
		rare_words=self.get_rare_words(processed_df)

		self.count_vectorizer=self.create_features(processed_df,rare_words)
		self.tweet_classifier,self.tfidf_transformer=self.tweet_classifier_model(self.count_vectorizer,processed_df)

	def get_data(self,file_name,categories):
		"""
		Getting all training data from a file and converting to a dataFrame
		input: file_name (String): File name where the training data exists
			   categories (Array (String)): Array of categories our documents belong to
		return: dataframe: containing each text and its rspective category
		"""

		training_set=[]
		for cat in categories:
			for each_doc in glob.glob(file_name%cat):
			
				with open(each_doc) as fi:
					training_set.append((cat,fi.read()))

		labels=['category','text']
		news_df=pd.DataFrame(training_set,columns=labels)
		return news_df

	def process(self,text):

		"""
		Processing Each Text for model purpose
		input: text (String): Text in the document
		return: Array: processed text converted into tokens
		"""
		try:
			common_tweet_stopwords=["rt","co","http","https"]
			stopwords=nltk.corpus.stopwords.words('english')+common_tweet_stopwords
			lemmatizer=nltk.stem.wordnet.WordNetLemmatizer()
			text=text.lower()
			text=text.replace('\'s','')
			text=text.replace('\'','')
			text = re.sub("\d+", "", text)

			tokens=[]
			for punct in string.punctuation:
				text=text.replace(punct,' ')       

			for token in nltk.word_tokenize(text):
				try:
					if token.encode() not in stopwords and len(token)>3:
						tokens.append(lemmatizer.lemmatize(token).encode())
				except:
					continue

			return tokens
		except:
			print "Problem Processing Text:",text
			return
		pass

	def process_all(self,df):
		"""
		Processing the dataframe texts
		input: df (dataframe): dataframe consisting each text doc with its category
		return: dataframe : Dataframe consisting of category and processed text
		"""

		lemmatizer=nltk.stem.wordnet.WordNetLemmatizer()
		for ix,article in df["text"].iteritems():
		    df['text'][ix]=self.process(article)
		return df
		pass

	def get_rare_words(self,processed_articles):
		"""
		Getting rare word. These are words which are very less often used, And so these wont be much relevant
		input: processed_articles (dataframe): The datframe which consists of categories and the processed text
		return: Array: rare words 
		"""

		c=Counter()
		all_articles=[]
		for articles in processed_articles["text"]:
		    all_articles.extend(articles)
		c.update(all_articles)
		rare_words=[(k) for (k,v) in sorted(c.items()) if v==1 ]
		return rare_words
		pass

	def create_features(self,processed_articles,rare_words):
		"""
		Creating a vector of features. These are the words remaining after processing and after removing stopwords
		input: processed_articles (dataframe): The dataframe which consists of categories and the processed text
			   rare_words : words which appear rarely
		return: Matrix: Token Counts in each document
		"""
		stopwords=nltk.corpus.stopwords.words('english')

		data=[(" ").join(t) for t in processed_articles["text"]]
		count_vectorizer = CountVectorizer(stop_words=(rare_words+[x.encode() for x in stopwords])).fit(data)
		return count_vectorizer
		pass

	def tweet_classifier_model(self,count_vectorizer,processed_df):
		"""
		input: count_vectorizer (matrix): Token Counts in each document
			   processed_df : dataframe with processed text
		return: (MNB model, Tfidf matrix): (Model with fit training data, Count Matrix transformed to TFIDF form)
		"""
		transformed_count_vectorizer=count_vectorizer.transform([" ".join(list_text) for list_text in processed_df["text"]])
		tfidf_transformer = TfidfTransformer().fit(transformed_count_vectorizer)
		articles_tfidf = tfidf_transformer.transform(transformed_count_vectorizer)
		tweet_classifier=MultinomialNB().fit(articles_tfidf, processed_df['category'])

		return (tweet_classifier,tfidf_transformer)
		pass

	def predict(self,processed_tweet):
		"""
		Predicting the category for a processed text
		input: processed_text (Array): Tokenised form of text which has been processed
		return: String: Category to which the text belongs to
		"""

		tweet_count_vectorizer=self.count_vectorizer.transform([" ".join(processed_tweet)])
		processed_tweet_final=self.tfidf_transformer.transform(tweet_count_vectorizer)

		return self.tweet_classifier.predict(processed_tweet_final)[0]
		pass
