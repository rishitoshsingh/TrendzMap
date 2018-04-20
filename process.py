from pyspark import SparkConf, SparkContext
# from pyspark.mllib.classification import  NaiveBayesModel
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext


import json
import MySQLdb
import googlemaps
gmaps = googlemaps.Client(key='AIzaSyDLeeuz2J1-KVSPXOiBd7hoBIQYXA9BIE4')
# gmaps = googlemaps.Client(key='AIzaSyD3x9NEdpq8zBGQmV6pXSlD3vACBNChKhQ')

def read_int(stream):
    length = stream.read(4)
    if not length:
        raise EOFError
    return struct.unpack("!i", length)[0]

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

            get_count = "SELECT country FROM trendz WHERE tag_mention = '%s' AND typ = '%s';" % (tag_mention,typ)
            cursor.execute(get_count)
            previous_country = str(cursor.fetchone()[0])

            if (previous_country == '' and not country == ''):
                update_query = "UPDATE trendz SET total_count = '%s' AND country = '%s' AND tweet_id = '%s' WHERE tag_mention = '%s' AND typ = '%s';" % (count,country,tweet_id,tag_mention,typ)
                cursor.execute(update_query)
            else:
                update_query = "UPDATE trendz SET total_count = '%s' AND tweet_id = '%s' WHERE tag_mention = '%s' AND typ = '%s';" % (count,tweet_id,tag_mention,typ)         
                cursor.execute(update_query)
            print("Updated")
        conn.commit()
    except MySQLdb.Error as e:
        print('-------------------------------------------------------------------MySQL ERROR--------------------------------------------------------------------------------------')
        print(e)
        print(typ)
        print(tag_mention)
        print(country)
        print('---------------------------------------------------------------------------------------------------------------------------------------------------------')
    finally:
        conn.close()

def process_tag(data):

        print("Processing RDD..........")        

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
                            # print("Found " + country + " for "+ user_location)
                                                    
                        except Exception as e:
                            print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++Geocoding API ERROR++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                            print(e)
                            print(user_location)
                            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                            pass
                        
                    for hastag in tag_lis:
                        insert_tag('#',str(hastag['text']),country,tweet_id)
                if (user_mention_lis):
                    if (tweet_location):
                        country = tweet_location['country']
                    else :
                        if (user_location == None):
                            print("location = None")
                            pass
                        try:
                            if(not country == ''):
                                country = extract_country(user_location)
                            # print("Found " + country + " for "+ user_location)

                        except Exception as e:
                            print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++Geocoding API ERROR++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                            print(e)
                            print(user_location)
                            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
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
ssc, topics = ['trendz'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
tweets = kstream.map(lambda x: json.loads(x[1]))

temp = tweets.map(lambda x: (x['entities']))
print(temp)

tags = tweets.map(lambda x: (x['entities']['hashtags'],x['entities']['user_mentions'],x['place'],x['user']['location'],x['id']))
# tags = tweets.map(lambda x: (x.entities['hashtags'],x.entities['user_mentions'],x['place'],x['user']['location'],x['id']))
tags.foreachRDD(process_tag)

ssc.start() 
ssc.awaitTerminationOrTimeout(1000)
ssc.stop(stopGraceFully = True)
