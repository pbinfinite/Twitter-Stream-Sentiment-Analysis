from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
    

def load_wordlist(filename):
	#This function returns a list or set of words from the given filename.	
    words = {}
    f = open(filename, 'rU')
    text = f.read()
    text = text.split('\n')
    for l in text:
    	words[l] = 1
    f.close()
    return words


def updateFunction(newval, currCt):
    if currCt is None:
    	currCt = 0
    return sum(newval, currCt) 


def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
    ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": '192.168.200.128:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii", "ignore"))

    # Each element of tweets will be the text of a tweet.
    # We keep track of a running total counts and print it at every time step.
    
    words = tweets.flatMap(lambda line:line.split(" "))
    
    positive = words.map(lambda word: ('Positive', 1) if word in pwords else ('Positive', 0))
    negative = words.map(lambda word: ('Negative', 1) if word in nwords else ('Negative', 0))
    sentiment = positive.union(negative)
    sentimentct = sentiment.reduceByKey(lambda x,y: x+y)
    currsenti_ct = sentimentct.updateStateByKey(updateFunction)
    currsenti_ct.pprint()
    
    # The counts variable hold the word counts for all time steps
    counts = []
    sentimentct.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))
    
    # Start the computation
    ssc.start() 
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully = True)

    return counts

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)

    # Creating a streaming context with batch interval of 10 sec
    ssc = StreamingContext(sc, 25)
    ssc.checkpoint("checkpoint")
    pwords = load_wordlist("./positive.txt")
    nwords = load_wordlist("./negative.txt")
    counts = stream(ssc, pwords, nwords, 100)

if __name__=="__main__":
    main()
