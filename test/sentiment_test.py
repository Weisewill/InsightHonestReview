# Import Spark NLP            
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.embeddings import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import map_values, expr, col, when, udf

conf = (SparkConf()
         .setAppName("S3 Configuration Test")
         .set("spark.executor.instances", "1")
         .set("spark.executor.cores", 1)
         .set("spark.executor.memory", "2g"))
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

# Start Spark Session with Spark NLP
spark = sparknlp.start()
spark.sparkContext.setLogLevel("ERROR")

#ispark = SparkSession.builder \
# .master('local[*]') \
# .appName('Spark NLP') \
# .config("spark.driver.memory", "6g").config("spark.executor.memory", "6g").config("spark.jars.packages", "JohnSnowLabs:spark-nlp:2.3.6,com.johnsnowlabs.nlp:spark-nlp-ocr_2.11:2.3.6,javax.media.jai:com.springsource.javax.media.jai.core:1.1.3").config("spark.jars.repositories", "http://repo.spring.io/plugins-release").getOrCreate()

#pipeline = PretrainedPipeline('explain_document_dl', lang='en')

pipeline = PipelineModel.load("./spark-nlp-models/analyze_sentiment_en_2.1.0_2.4_1563204637489/")

# Your testing dataset
text = [(1, 3, "The Mona Lisa is a 16th century oil painting created by Leonardo. It's held at the Louvre in Paris."), \
        (2, 3, "Google has announced the release of a beta version of the popular TensorFlow machine learning library"),\
        (3, 4, "The Paris metro will soon enter the 21st century, ditching single-use paper tickets for rechargeable electronic cards."),\
        (4, 5, "The new Star Wars movie is the best movie, and I like it so much!")]

data = spark.createDataFrame(text, ["idx","score","text"])

# Annotate your testing dataset
result = pipeline.transform(data)

#print(list(result.keys()))

result.registerTempTable('result')
sentiment = sqlContext.sql('SELECT text, sentiment FROM result')

sentiment.show()
sentiment.printSchema()

#sentiment = sentiment.select("sentiment.metadata")
sentiment = sentiment.withColumn("confidence", expr('transform(sentiment.metadata, x -> map_values(x))'))
sentiment = sentiment.withColumn("result", sentiment.sentiment.result)

determineScoreUdf = udf(lambda x: 1 if x == "POSITIVE" else ( -1 if x == "NEGATIVE" else 0 ))
sentiment = sentiment.withColumn("result_score", determineScoreUdf("result") ) 

#sentiment = sentiment.withColumn("result_score",  expr( 'transform(sentiment.result, x -> when(x=="POSITIVE", 1).when(x=="NEGATIVE", -1).otherwise(0) )') )  

sentiment.printSchema()

sentiment.show()

print(result.count())

