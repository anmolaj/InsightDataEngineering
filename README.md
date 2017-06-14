###InsightDataEngineering
##Data Engineering Project at Insight

Below are basic instructions to understand initially

##Kafka:
1. Install Zookeeper
2. pip install kafka-python / pip install pykafka
3  install hadoop
4  install spark
#Delete a Topic:
/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic <-Your-topic->
#Code Reference:
1. http://kafka-python.readthedocs.io/en/master/usage.html

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

#After SSHing
sudo pip install kafka-python
create Topic - /usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic tryTopic --partitions 4 --replication-factor 2