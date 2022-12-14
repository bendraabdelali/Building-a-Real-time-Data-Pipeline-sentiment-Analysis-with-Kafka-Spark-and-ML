# Import libraries
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover, CountVectorizer, VectorAssembler
from pyspark.ml.classification import LinearSVC
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

kafka_topic_name = "topicA"
kafka_bootstrap_servers = 'kafka:9092'


spark = SparkSession \
        .builder \
        .appName("Sentiment Analysis") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0')\
        .master("local[*]") \
        .getOrCreate()  
    
df = spark \
     .readStream \
     .format("kafka") \
     .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
     .option("subscribe", kafka_topic_name) \
     .option("startingOffsets", "earliest") \
     .load()\
     .selectExpr("CAST(value AS STRING)") 

schema = "SentimentText STRING"               

df = df\
     .select(from_csv(col("value"),schema)\
     .alias("Sentiment"))      
df.printSchema()

df = df.select("Sentiment.*") 
df.printSchema()

df = df.select("SentimentText")
df.printSchema()
# Load a pipeline model
model = PipelineModel.load('./ML/SentimentAnalysis')

# Use the model to make predictions on streaming data
predictions = model.transform(df)
predictions.printSchema()
predictionFinal =  predictions.select(
                                "SentimentText", "prediction")
predictionFinal.printSchema()
# Output streaming predictions to the console
predictionFinal = predictionFinal.select( struct('SentimentText', 'prediction').alias('value'))
predictionFinal.printSchema()
# predictionFinal.writeStream.format('console').outputMode('append').start().awaitTermination()

predictionFinal.writeStream.format('csv')\
           .option("checkpointLocation", "/tmp/pyspark6/")\
           .option('path', '/output').start()
# Write the streaming data to a Kafka topic with security options

# predictionFinal.selectExpr("CAST(value AS STRING)") \
# .writeStream.format('kafka')\
# .option('kafka.bootstrap.servers', "kafkas:9092")\
# .option("checkpointLocation", "/tmp/pyspark6/")\
# .option('topic', 'topicB').start()