from elasticsearch import Elasticsearch
from pyspark import SparkContext, SparkConf
import glob


from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import *


# Enter your Elastic search Cluster nodes
es_cluster=["ec2-34-228-107-60.compute-1.amazonaws.com","ec2-34-198-227-176.compute-1.amazonaws.com","ec2-34-226-168-68.compute-1.amazonaws.com"]

es=Elasticsearch(es_cluster,http_auth=('elastic','changeme'))

def store_doc():
	"""
	Storing Each wiki doc in Elastic search database
	"""
	es.create(index="tech",doc_type='classified',id=hashlib.md5("A").hexdigest(),body={"title":"A","content":"My name is Anmol"})
	es.create(index="entertainment",doc_type='classified',id=hashlib.md5("B").hexdigest(),body={"title":"B","content":"I am funny"})



if __name__ == "__main__":


	from TextClassification import ClassifyText
	repartition_number=100
	
	file_name="s3n://wiki-data-insight/wikiData/enwiki-20170601-pages-meta-current1.xml-p10p30303"

		
	conf = SparkConf().setAppName('WikiFile_Processing')
	conf.setExecutorEnv('PYTHONPATH','pyspark.zip:py4j-0.8.2.1-src.zip')
	sc = SparkContext(conf=conf,pyFiles=['TextClassification.py'])

	sc.setLogLevel("WARN")
	classify=ClassifyText()
	

	sqlContext = SQLContext(sc)

	df = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='page').load(file_name)
	df.printSchema()

	print "----read----allfiles----------------------------"
	files_RDD=sc.wholeTextFiles(file_name)
	
	def text_Category(text):
		"""
		Predicting the category of each document
		"""
		#return len(text)
		return str(classify.predict(classify.process(text)))
	
	udf_text_Category = udf(text_Category, StringType())
	df=df.select(df['title'], (df['revision']['text']['_Value']).alias('wiki_text')).withColumn("category", udf_text_Category("wiki_text"))
	print df.show(2)
	







	
