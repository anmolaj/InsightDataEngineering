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

def store_doc(row):
	"""
	Storing Each wiki doc in Elastic search database
	"""
	#list_json=[]
	#for row in rdd:
		#row=[str(c) for c in rows]
	data = {"_index":row[2],"_type":"classified","_source":json.dumps({"title":row[0],"content":row[1]})}
		#list_json.append(data)
	#helpers.bulk(es,data)
	#es.create(index=rdd_Row[2],doc_type='classified',body=json.dumps({"title":rdd_Row[0],"content":rdd_Row[1]}))

	return data
	# print rdd_Row
	# es.create(index=rdd_Row[2],doc_type='classified',id=hashlib.md5(rdd_Row[0]).hexdigest(),body=json.dumps({"title":rdd_Row[0],"content":rdd_Row[1]}))
	# #es.create(index="entertainment",doc_type='classified',id=hashlib.md5("B").hexdigest(),body={"title":"B","content":"I am funny"})

def convert_doc(row):
	rdd_Row=[str(c) for c in row]
	#,id=hashlib.md5(rdd_Row[0]).hexdigest()
	#data = {"_index":rdd_Row[1],"_id":hashlib.md5(rdd_Row[0]).hexdigest(),"_source":json.dumps({"title":rdd_Row[0],"content":rdd_Row[1]})}
	es.create(index=rdd_Row[2],doc_type='classified',body=json.dumps({"title":rdd_Row[0],"content":rdd_Row[1]}))

	return rdd_Row

if __name__ == "__main__":


	from TextClassification import ClassifyText
	repartition_number=100
	
	#s3n://wiki-data-insight/wikiData/enwiki-20170601-pages-meta-current1.xml-p10p30303
	file_name="s3n://wiki-data-insight/wikiData/enwiki-20170601-pages-meta-current11.xml-p3046514p3926861"

	
		
	conf = SparkConf().setAppName('WikiFile_Processing')
	conf.setExecutorEnv('PYTHONPATH','pyspark.zip:py4j-0.8.2.1-src.zip')
	#conf.set("es.nodes", ",".join(es_cluster))
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
	df=df.select(df['title'], (df['revision']['text']['_Value']).alias('content')).withColumn("index", udf_text_Category("content"))


	print "-----WRITING---------------------"

	#df=df.rdd.map(list)
	def get_tuple(rdd):
		try:
			x= [str(c.encode('utf-8')) for c in rdd]
			#return (x[0],x[1],x[2],1
			return (x[0],x[1],x[2])
		except:
			return

	#df=df.rdd.map(lambda row: ([str(c) for c in row]))
	#df=df.rdd.map(list)
	#df.first()
	def store_it(rdd_Row):
		es.create(index=rdd_Row[2],doc_type='classified',body=json.dumps({"title":rdd_Row[0],"content":rdd_Row[1]}))



	df=df.rdd
	print df.first()
	df=df.map(lambda row: get_tuple(row))#.map(store_doc)
	#df.foreach(lambda x: store_it(x))
	#.map(store_doc)#.filter(lambda x:x[3]==1)
	#df.foreachPartition(store_doc)
	
	print df.first()
	#df.foreachRDD(lambda rdd: rdd.foreachPartition(store_doc))
	#df=df.map(convert_doc)
	#print df.first()
	#df=df.map(convert_doc)
	# # print df.first()
	for rdd_Row in df.toLocalIterator():
		try:
			es.create(index=rdd_Row[2],id=hashlib.md5(rdd_Row[0]).hexdigest(),doc_type='classified',body=json.dumps({"title":rdd_Row[0],"content":rdd_Row[1]}))
		except:
			continue
	#listX=[x for x in df.toLocalIterator()]

	#print listX[:5]

	#helpers.bulk(es,listX)
	# es_conf = {"es.nodes" : "ec2-34-205-123-236.compute-1.amazonaws.com","es.port" : "9200",'es.net.http.auth.user':'elastic','es.net.http.auth.pass':'changeme',"es.resource" : "tech/classified"} 
	# df.saveAsNewAPIHadoopFile(path='-',outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",keyClass="org.apache.hadoop.io.NullWritable",valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",conf=es_conf)
	# df2=df.map(lambda x: len(x))
	# print df2.first()
	# print df2.take(10)
	#print df.take(10)
	print "SAVING________"
	#df.map(store_doc)

	# df=df.map(convert_doc)
	# print df.first()
	#helpers.bulk(es,listX)
	

 	print "CONVERTING TO RDD____________"
	# rdd = df.rdd.map(tuple)
	# rdd.take(2)
	
	#df.saveToES('{index}/classified')

	#df.show(2)
	







	
