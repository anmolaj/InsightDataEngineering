from elasticsearch import Elasticsearch
from elasticsearch import helpers
from pyspark import SparkContext, SparkConf
import glob
import json
import hashlib



# import org.apache.spark.sql.SQLContext    
# import org.apache.spark.sql.SQLContext._

# import org.elasticsearch.spark.sql._

# import org.elasticsearch.spark.sql
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import *


es_cluster=["ec2-34-205-123-236.compute-1.amazonaws.com:9200","ec2-34-226-76-219.compute-1.amazonaws.com:9200","ec2-34-226-104-234.compute-1.amazonaws.com:9200"]

es=Elasticsearch(es_cluster,http_auth=('elastic','changeme'))


if __name__ == "__main__":


	from TextClassification import ClassifyText
	
	
	file_name="<Enter-S3-file-here>"

	
		
	conf = SparkConf().setAppName('WikiFile_Processing')
	conf.setExecutorEnv('PYTHONPATH','pyspark.zip:py4j-0.8.2.1-src.zip')

	sc = SparkContext(conf=conf,pyFiles=['TextClassification.py'])

	sc.setLogLevel("WARN")
	classify=ClassifyText()
	

	sqlContext = SQLContext(sc)

	df = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='page').load(file_name)
	df.printSchema()


	
	def text_Category(text):
		"""
		Predicting the category of each document
		"""
		return str(classify.predict(classify.process(text)))
	
	udf_text_Category = udf(text_Category, StringType())
	df=df.select(df['title'], (df['revision']['text']['_Value']).alias('content')).withColumn("index", udf_text_Category("content"))


	print "-----WRITING---------------------"

	
	def get_tuple(rdd):
		try:
			"""
			Converting each row into a readable tuple
			"""
			x= [str(c.encode('utf-8')) for c in rdd]
			
			return (x[0],x[1],x[2])
		except:
			return

	
	def store_it(rdd_Row):
		es.create(index=rdd_Row[2],doc_type='classified',body=json.dumps({"title":rdd_Row[0],"content":rdd_Row[1]}))



	df=df.rdd
	print df.first()
	df=df.map(lambda row: get_tuple(row))
	
	
	print df.first()
	
	for rdd_Row in df.toLocalIterator():
		try:
			es.create(index=rdd_Row[2],id=hashlib.md5(rdd_Row[0]).hexdigest(),doc_type='classified',body=json.dumps({"title":rdd_Row[0],"content":rdd_Row[1]}))
		except:
			continue
	
	)
	







	
