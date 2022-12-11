# Import libraries
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover, CountVectorizer, VectorAssembler, SelectKBest
from pyspark.ml.classification import LinearSVC
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder.appName('streaming').getOrCreate()

# Create a streaming DataFrame from Kafka
df = spark.readStream.format('kafka')\
     .option('kafka.bootstrap.servers', 'localhost:9092')\
     .option('subscribe', 'topicA')\
     .load().selectExpr("CAST(value AS STRING)") 

orders_schema = StructType() \
        .add("ItemID", IntegerType())\
        .add("SentimentSource", StringType()) \
        .add("SentimentText", StringType()) \
        
df = df\
        .select(from_json(col("value"),orders_schema)\
        .alias("Sentiment"))      

df = orders_df2.select("Sentiment.*")   

# Load a pipeline model
model = PipelineModel.load('../ML/SentimentAnalysis')

# Use the model to make predictions on streaming data
predictions = model.transform(df)

# Output streaming predictions to the console
predictions.writeStream.format('console').start().awaitTermination()
