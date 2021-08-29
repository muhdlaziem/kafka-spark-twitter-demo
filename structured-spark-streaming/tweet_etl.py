import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from textblob import TextBlob


# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity


def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity


def select_id(text):
    try:
        text_dict = json.loads(text)
        return text_dict['id']

    except Exception as e:
        print(f"Bad Data: {e}")
        return ""


def select_tweet(text):
    try:
        text_dict = json.loads(text)
        return text_dict['text']

    except Exception as e:
        print(f"Bad Data: {e}")
        return ""


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Simple Twitter ETL") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .config("spark.sql.streaming.checkpointLocation", "/home/muhdlaziem/chk") \
        .getOrCreate()

    polarity_detection_udf = udf(polarity_detection, StringType())
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    select_key = udf(select_id, StringType())
    select_tweet_udf = udf(select_tweet, StringType())

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "tweets") \
        .option("startingOffsets", "earliest") \
        .load()

    df.selectExpr("CAST(value AS STRING)") \
        .withColumn("key", select_key("value")) \
        .withColumn("value", select_tweet_udf("value")) \
        .withColumn('value', regexp_replace('value', r'http\S+', '')) \
        .withColumn('value', regexp_replace('value', '@\w+', '')) \
        .withColumn('value', regexp_replace('value', '#', '')) \
        .withColumn('value', regexp_replace('value', 'RT', '')) \
        .withColumn('value', regexp_replace('value', ':', '')) \
        .withColumn("polarity", polarity_detection_udf("value")) \
        .withColumn("subjectivity", subjectivity_detection_udf("value")) \
        .select("key", to_json(struct('value', 'polarity', 'subjectivity')).alias('value')) \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "sentiment_tweets") \
        .start() \
        .awaitTermination()
