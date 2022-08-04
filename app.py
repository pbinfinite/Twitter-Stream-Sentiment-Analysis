import json
from kafka import SimpleProducer, KafkaClient
import tweepy

#read twitter stream and push to kafka
class TweeterStreamListener(tweepy.StreamListener):

    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()
        client = KafkaClient("192.168.200.128:9092")
        self.producer = SimpleProducer(client, async = True, batch_send_every_n = 1000, batch_send_every_t = 25) 
        '''batches of messages are of size 1000 and these messages are sent every 25 seconds (even if the no.of messages <1000)'''

    def on_status(self, status):
    #Called on new data. Pushed to kafka queue
        msg =  status.text.encode('utf-8')
        try:
            self.producer.send_messages(b'twitterstream', msg)
        except Exception as e:
            print(e)
            return False
        return True

    def on_error(self, status_code):
        print("Error in kafka")
        return True # Don't kill the stream

    def on_timeout(self):
        return True # Don't kill the stream

if __name__ == '__main__':

    consumer_key = "consumerkey"
    consumer_secret = "consumersecret"
    access_key = "accesskey"
    access_secret = "accesssecret"

    #Auth object
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)

    # Create stream and bind the listener to it
    stream = tweepy.Stream(auth, listener = TweeterStreamListener(api))


    #stream.filter(track=['python','c++'], languages = ['en'])
    stream.filter(track=['games'],languages=['en'])
