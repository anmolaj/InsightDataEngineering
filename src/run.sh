/usr/local/spark/bin/spark-submit --master  local --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 ./sparkConsumer.py

/usr/local/spark/bin/spark-submit --master  spark://ec2-35-162-144-195.us-west-2.compute.amazonaws.com:7077  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 ./sparkConsumer.py