import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

access_token = "3050035218-x7KuAMHG3C2Yrk2qbrkMEDKbnN2ogEHrrVSp9Ar"
access_token_secret =  "un09OqOjQaBDO53e9iAZsqTE2cqMZzPduWNMZndTBnNAe"
consumer_key =  "UvZkmFhM1wTp5t8MFRFEJiDpj"
consumer_secret =  "stOmR2FAAlWGEljCH7ccyUqShu97i4H96wBHmWpC5R1GK4zMfe"

class MyStreamListener(StreamListener):
    def on_data(self, data):
    	
    	print(data)
    	# for tweets in data:
    	# 	for tags in tweets.entities["hashtags"]:
    	# 		print(tags["text"])
    	producer.send_messages("twitterstream", data.encode('utf-8'))
    	## print (data)
    	return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
streamListner = MyStreamListener()
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = tweepy.Stream(auth, streamListner)
stream.filter(track="#")


