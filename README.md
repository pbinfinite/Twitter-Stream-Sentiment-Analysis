# Twitter-Stream-Sentiment-Analysis
<br>
This project performs sentiment analysis on real-time stream of tweets obtained using the Twitter API.<br>
The stream of tweets is filtered using specific words or hashtags.<br>
Credentials to access the twitter API can be obtained by creating a twitter developer account.<br><br>
Here, Kafka is used as a queuing service for the stream of tweets.<br>
After creation of a kafka topic named twitterstream, we enable kafka and zookeeper using the commands:<br><br>
<i>sudo systemctl enable zookeeper</i><br>
<i>sudo systemctl enable kafka<br></i><br>
To check the status: <i>sudo systemctl status kafka zookeeper</i><br>
<br>
Simultaneously, start kafka consumer at cd /usr/local/kafka-server/bin
<br><br><i>
./kafka-console-consumer.sh --zookeeper 192.168.200.128:2181 --topic twitterstream --from-beginning</i>
<br><br>
For stream analysis, start spark submit in project directory,<br><br><i>
$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 twitterStream.py</i><br>
