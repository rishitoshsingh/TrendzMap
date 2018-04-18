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
import googlemaps
# gmaps = googlemaps.Client(key='AIzaSyDLeeuz2J1-KVSPXOiBd7hoBIQYXA9BIE4')
gmaps = googlemaps.Client(key='AIzaSyD3x9NEdpq8zBGQmV6pXSlD3vACBNChKhQ')

def extract_country(location):
    geocode_result = gmaps.geocode(location)
    address_list = geocode_result[0]['address_components']
    for item in address_list:
        if (item['types'][0] == 'country'):
            country = item['long_name']
            return country
        else:
            pass
    return 0


def insert_tag(typ,tag_mention,country,tweet_id):
    # query = "INSERT INTO tags(tag) VALUES ('%s');" % tag
    query = "INSERT INTO trendz(typ,tag_mention,country,tweet_id) VALUES ('%s','%s','%s','%s');" % (typ,tag_mention,country,tweet_id)
    try:
        conn = MySQLdb.connect("localhost","rishi","","trendzmap",charset="utf8mb4")
        cursor = conn.cursor()
        # count_query = "SELECT tag FROM tags WHERE tag = '%s';" % tag
        count_query = "SELECT tag_mention FROM trendz WHERE tag_mention = '%s' AND typ = '%s';" % (tag_mention,typ)
        cursor.execute(count_query)
        row = cursor.rowcount
        if(row is 0):
            cursor.execute(query)
            print("Inserted")
        else:
            # get_count = "SELECT total_count FROM tags WHERE tag = '%s';" % (tag)
            get_count = "SELECT total_count FROM trendz WHERE tag_mention = '%s' AND typ = '%s';" % (tag_mention,typ)
            cursor.execute(get_count)
            count = cursor.fetchone()[0]
            count+=1
            update_query = "UPDATE trendz SET total_count = '%s' WHERE tag_mention = '%s' AND typ = '%s';" % (count,tag_mention,typ)         
            cursor.execute(update_query)
            print("Updated")
        conn.commit()
    except MySQLdb.Error as e:
        print('---------------------------------------------------------------------------------------------------------------------------------------------------------')
        print(e)
        print(typ)
        print(tag_mention)
        print(country)
        print('---------------------------------------------------------------------------------------------------------------------------------------------------------')
    finally:
        conn.close()

def insert_tweet(tweet,username,tweet_id):
    query = "INSERT INTO tweets(tweet,username,tweet_id) VALUES ('%s','%s','%s');" % (str(tweet),username,str(tweet_id))
    try:
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
                # temp[i].append(p.encode('utf-8','ignore'))
                temp[i].append(p)
                temp[i].append(q)
                temp[i].append(r)
                i+=1
            for i in temp:
                insert_tweet(str(i[0]),str(i[1]),int(i[2]))
        else:
            print("Empty RDD !!!")        
            pass


# def process_tag(tag):

#         print("Processing data ...")        

#         if (not tag.isEmpty()):
#             temp = []
#             for lis in tag.collect():
#                 if (lis):
#                     for hastag in lis:
#                         insert_tag(str(hastag['text']))        
#         else:
#             print("Empty RDD !!!")        
#             pass


def process_tag(data):

        print("Processing data ...")        

        if (not data.isEmpty()):
            temp = []
            for tag_lis,user_mention_lis,tweet_location,user_location,tweet_id in data.collect():
                country = ''
                if (tag_lis):
                    if (tweet_location):
                        country = tweet_location['country']
                    else :
                        if (user_location == None):
                            print("location = None")
                            pass
                        try:
                            country = extract_country(user_location)
                            print("Found " + country + " for "+ user_location)
                                                    
                        except Exception as e:
                            print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                            print(e)
                            print(len(user_location))
                            pass
                        
                    for hastag in tag_lis:
                        insert_tag('#',str(hastag['text']),country,tweet_id)
                country = ''
                if (user_mention_lis):
                    if (tweet_location):
                        country = tweet_location['country']
                    else :
                        if (user_location == None):
                            print("location = None")
                            pass
                        try:
                            country = extract_country(user_location)
                            print("Found " + country + " for "+ user_location)
                                                       
                        except Exception as e:
                            print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                            print(e)
                            print(len(user_location))
                            pass
                    for mention in user_mention_lis:
                        insert_tag('@',str(mention['screen_name']),country,tweet_id)
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
tags = tweets.map(lambda x: (x['entities']['hashtags'],x['entities']['user_mentions'],x['place'],x['user']['location'],x['id']))
# txt.foreachRDD(process_data)
tags.foreachRDD(process_tag)

ssc.start() 
ssc.awaitTerminationOrTimeout(1000)
ssc.stop(stopGraceFully = True)
