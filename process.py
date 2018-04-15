from pyspark import SparkConf, SparkContext
# from pyspark.mllib.classification import  NaiveBayesModel
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext

import operator
import json
# import mysqlclient as MySQLdb
# import mysqlclient 
import MySQLdb
# from mysql.connector import MySQLConnection, Error
# from python_mysql_dbconfig import read_db_config

def insert_tag(tag):
    query = "INSERT INTO tags(tag) VALUES ('%s');" % tag
    try:
        conn = MySQLdb.connect("localhost","rishi","","trendzmap",charset="utf8mb4")
        cursor = conn.cursor()
        count_query = "SELECT tag FROM tags WHERE tag = '%s';" % tag
        cursor.execute(count_query)
        row = cursor.rowcount
        if(row is 0):
            cursor.execute(query)
            print("Tag Inserted")
        else:
            get_count = "SELECT total_count FROM tags WHERE tag = '%s';" % (tag)
            cursor.execute(get_count)
            count = cursor.fetchone()[0]
            count+=1
            update_query = "UPDATE tags SET total_count = '%s' WHERE TAG = '%s';" % (count,tag)         
            cursor.execute(update_query)
            print("Tag Updated")
        conn.commit()
    except MySQLdb.Error as e:
        print(e)
        print(tag)
    finally:
        conn.close()

def insert_tweet(tweet,username,tweet_id):
    #query = "INSERT INTO tweets(tweet,username,pnr,prediction,tweet_id) VALUES ('%s','%s',%s,%s,%s);" % (tweet,username,str(pnr),str(int(prediction)))
    #query = "INSERT INTO tweets(tweet,username,pnr,prediction,tweet_id) VALUES ('"+tweet+"','"+username+"',"+str(pnr)+","+str(int(prediction))+","+str(tweet_id)+");"
    # query = "INSERT INTO tweets(tweet, username, tweet_id) VALUES ('"+tweet+"','"+username+"',"+str(tweet_id)+");"
    query = "INSERT INTO tweets(tweet,username,tweet_id) VALUES ('%s','%s','%s');" % (str(tweet),username,str(tweet_id))
    try:
        # conn = MySQLdb.connect("localhost","rishi","","twitter")
        conn = MySQLdb.connect("localhost","rishi","","trendzmap",charset="utf8mb4")
        cursor = conn.cursor()
        cursor.execute(query)
        print("Database insertion SUCCESSFUL!!")
        conn.commit()
    except MySQLdb.Error as e:
        print(e)
        print(tweet)
        print(username)
        print(tweet_id)
        print("Database insertion unsuccessful!!")
    finally:
        conn.close()


def process_data(data):

        print("Processing data ...")        

        if (not data.isEmpty()):
            temp = []
            i=0
            for p,q,r in data.collect():
                temp.append([])
                temp[i].append(p)
                temp[i].append(q)
                temp[i].append(r)
                i+=1
            for i in temp:
                insert_tweet(str(i[0]),str(i[1]),int(i[2]))
        else:
            print("Empty RDD !!!")        
            pass

def process_tag(tag):

        print("Processing data ...")        

        if (not tag.isEmpty()):
            temp = []
            for lis in tag.collect():
                if (lis):
                    for hastag in lis:
                        insert_tag(str(hastag['text']))        
        else:
            print("Empty RDD !!!")        
            pass


conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
val = sc.parallelize("abd")


ssc = StreamingContext(sc, 10)
ssc.checkpoint("checkpoint")
kstream = KafkaUtils.createDirectStream(
ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
tweets = kstream.map(lambda x: json.loads(x[1]))
twitter=tweets.map(lambda tweet: tweet['user']['screen_name'])
tweet_text = tweets.map(lambda tweet: tweet['text'])

txt = tweets.map(lambda x: (x['text'], x['user']['screen_name'], x['id']))
tags = tweets.map(lambda x: (x['entities']['hashtags']))
txt.foreachRDD(process_data)
tags.foreachRDD(process_tag)

ssc.start() 
ssc.awaitTerminationOrTimeout(1000)
ssc.stop(stopGraceFully = True)
