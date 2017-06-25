# Data Engineering Project at Insight

## TweetoPedia
### Table of Contents

1. [Overview](README.md#overview)
2. [Batch](README.md#batch)
3. [Streaming](README.md#streaming)
4. [Pipeline](README.md#pipeline)
5. [Repository Structure](README.md#repository-structure)

### Overview

TweetoPedia is a project developed as a part of my Data Engineering fellowship at Insight Data Science
This project is to help General Audience and Companies in the following way:

GENERAL AUDIENCE:
People are alwasy talking about something interesting in Twitter. You can definitely get whats trending but wouldn't it be more comfortable to get something in the sraea of your interest. And even if you get it what next. Its not necessary that you might have information on it.
For this purpose, My application TweetoPedia helps you to find current trending words in your area of interest i.e sports, business, entertainment, politics, technology.

COMPANIES:
What if companies could advertise on wikipedia pages. This application will help companies to find trending words in their domain and then find relevant wikipedia pages to advertise their content on.

To understand the working:
The Flask based UI helps a user to select the category of his choice. Once selected, the user is able to see a list of top 10 words which have been used the most in last 5 minutes. This list is uopdated dynamically. 
The user can then select a word from this list which he most interested in and can find appropriate Wikipedia Titles

The whole project had 2 aspects in implementation which are further discussed below
### Batch Processing






Below are basic instructions to understand initially

## Change Install File
Change Kafka version from 0.10.1.1 - 0.8.2.1

##Technologies to install:

(Tweets is the Tag_name)
peg install Tweets ssh
peg install Tweets aws
peg install Tweets hadoop
peg service Tweets hadoop start

peg install Tweets zookeeper
peg service Tweets zookeeper start

peg install Tweets spark
peg service Tweets spark start

peg install Tweets kafka
peg service Tweets kafka start

peg install Tweets elasticsearch

peg install Wiki ssh
peg install Wiki aws
peg install Wiki hadoop
peg service Wiki hadoop start
peg install Wiki spark
peg service Wiki spark start

peg install Wiki elasticsearch
peg service Wiki elasticsearch start

peg install ES elasticsearch
peg service ES elasticsearch start

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
sudo pip install nltk
sudo python -m nltk.downloader all
sudo python -m nltk.downloader punkt
sudo python -m nltk.downloader wordnet

Update python:
sudo add-apt-repository ppa:fkrull/deadsnakes-python2.7
sudo apt-get update 
sudo apt-get install python2.7   --This uodates to python 2.7.12

Start Zookeeper
nohup /usr/local/kafka/bin/zookeeper-server-start.sh config/zookeeper.properties &

Start kafka server:
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper ip-10-0-0-4:2181,ip-10-0-0-6:2181 --replication-factor 3 --partitions 2 --topic tweet-topic
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 2 --topic tweet-topic2

/usr/local/kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic tweet-topic2

Delete a Topic:
/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic tweet-topic2

Copying pkl to consumer:
 scp -i /Users/anmoljain/Desktop/Insight/anmolj-E1.pem /Users/anmoljain/Desktop/Insight/Clustering/twitter.pkl ubuntu@ec2-34-225-80-111.compute-1.amazonaws.com:

 scp -r -i /Users/anmoljain/Desktop/Insight/anmolj-E1.pem  /Users/anmoljain/Desktop/Insight/Clustering/Training/bbc2 ubuntu@ec2-34-225-80-111.compute-1.amazonaws.com:

 #Wikipedia cluster

  scp -r -i /Users/anmoljain/Desktop/Insight/anmolj-E1.pem  /Users/anmoljain/Desktop/Insight/Clustering/Training/bbc2 ubuntu@ec2-34-228-107-60.compute-1.amazonaws.com:

  scp -r -i /Users/anmoljain/Desktop/Insight/anmolj-E1.pem  /Users/anmoljain/Desktop/Insight/Dashboard/flask ubuntu@ec2-34-225-80-111.compute-1.amazonaws.com:

	scp -r -i /Users/anmoljain/Desktop/Insight/anmolj-E1.pem  /Users/anmoljain/Desktop/Insight/InsightDataEngineeringProject/ClassifyText.py ubuntu@ec2-34-228-107-60.compute-1.amazonaws.com:

#Code Reference:
1. http://kafka-python.readthedocs.io/en/master/usage.html
2. https://github.com/apache/spark/blob/master/examples/src/main/python/streaming/kafka_wordcount.py



