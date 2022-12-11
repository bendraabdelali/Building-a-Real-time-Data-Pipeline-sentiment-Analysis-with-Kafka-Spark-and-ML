# Import libraries
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover, CountVectorizer, VectorAssembler, SelectKBest
from pyspark.ml.classification import LinearSVC
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import shutil

def Accuracy(predictions):
    predictionFinal =  predictions.select(
        "MeaningfulWords", "prediction", "Label")
    # accuracy
    correctPrediction = predictionFinal.filter(
        predictionFinal['prediction'] == predictionFinal['Label']).count()
    totalData = predictionFinal.count()
    return correctPrediction/totalData  

# Create a SparkSession
spark = SparkSession.builder.appName('streaming').getOrCreate()

# Load a pipeline model
model = PipelineModel.load('../ML/SentimentAnalysis')

# Create a streaming DataFrame from a folder
df = spark.readStream.csv('/ML/dataset/', option('latestFirst', 'true'))

df = df.select("SentimentText", col("Sentiment").cast("Int").alias("label"))

dividedData = df.randomSplit([0.7, 0.3]) 
trainingData = dividedData[0] 
testingData = dividedData[1] 

# Create a tokenizer to split the SentimentText into individual words
tokenizer = Tokenizer(inputCol='SentimentText', outputCol='words')

# Create a stop words remover
remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol='filtered_words')

# Create a CountVectorizer or HashingTF to convert the words into numerical values
cv = CountVectorizer(inputCol=remover.getOutputCol(), outputCol='features')

# Use a feature selector to select the relevant fields
selector = VectorAssembler(inputCols=['features'], outputCol='selected_features')

# Create a classification model
classifier = LinearSVC(featuresCol=selector.getOutputCol(),labelCol='label' )

# Put everything in a pipeline
pipeline = Pipeline(stages=[tokenizer, remover, cv, selector, classifier])

# Fit the model to the data
NewModel = pipeline.fit(trainingData)

Latestpredictions = NewModel.transform(testingData)

predictions = model.transform(testingData)

NewAccuracy = Accuracy(Latestpredictions)
OldAccuracy = Accuracy(predictions)

# Compare the evaluation metrics of the two models
if NewAccuracy > OldAccuracy :
  # Save the new model if it has a higher accuracy than the old model
  shutil.rmtree('../ML/SentimentAnalysis')
  NewModel.save('../ML/SentimentAnalysis')

df = df.select("*").count()

df.writeStream.format('console').start()



