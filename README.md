###InsightDataEngineering

##Data Engineering Project at Insight

Below are basic instructions to understand initially

## Change Install File
Change Kafka version from 0.10.1.1 - 0.8.2.1

##Technologies to install:

(Tweets is the Tag_name)
peg install Tweets ssh
peg install Tweets aws
peg install Tweets hadoop
peg service Tweets hadoop start
peg install Tweets spark
peg service Tweets spark start
peg install Tweets zookeeper
peg service Tweets zookeeper start
peg install Tweets kafka
peg service Tweets kafka start

##Kafka:

1. Install Zookeeper
2. pip install kafka-python / pip install pykafka
3  install hadoop
4  install spark



#After SSHing
change broker id
sudo vi /usr/local/kafka/config/server.properties

sudo pip install kafka-python
sudo pip install tweepy

Update python:
sudo add-apt-repository ppa:fkrull/deadsnakes-python2.7
sudo apt-get update 
sudo apt-get install python2.7

Start kafka server:
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 2 --topic tweet-topic

/usr/local/kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic tweet-topic

Delete a Topic:
/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic tweet-topic

#Code Reference:
1. http://kafka-python.readthedocs.io/en/master/usage.html
2. https://github.com/apache/spark/blob/master/examples/src/main/python/streaming/kafka_wordcount.py

