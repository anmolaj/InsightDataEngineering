/usr/local/spark/bin/spark-submit --master  local[*] --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 ./sparkConsumer.py

/usr/local/spark/bin/spark-submit --master  spark://ec2-34-227-214-255.compute-1.amazonaws.com:7077  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 ./sparkConsumer.py


/usr/local/spark/bin/spark-submit  --master  spark://ec2-34-228-107-60.compute-1.amazonaws.com:7077  --jars /home/ubuntu/elasticsearch-hadoop-2.0.2.jar --packages com.databricks:spark-xml_2.10:0.4.1 ./wikiSparkES.py  

/usr/local/spark/bin/spark-submit  --master  spark://ec2-34-228-107-60.compute-1.amazonaws.com:7077  --jars /home/ubuntu/elasticsearch-hadoop-2.0.2.jar --packages org.apache.spark.sql ./wikiSparkES.py  