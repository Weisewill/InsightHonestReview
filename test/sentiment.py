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
from pyspark.sql.types import *

# Configure spark SQL
conf = (SparkConf() \
        .setAppName("Process") \
        .set("spark.shuffle.service.enabled", True) \
        .set("spark.dynamicAllocation.enabled", True) \
        .set("spark.executor.instances", "1") \
        .set("spark.driver.cores", 2) \
        .set("spark.executor.memory", "1024m"))
sc = SparkContext(conf=conf)
#sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName('Reddit Comments ETL').getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")


# Start Spark Session with Spark NLP
#spark = sparknlp.start()
#spark.sparkContext.setLogLevel("ERROR")


def readVGNames(path):
    df = sqlContext.read.option("header", "true").csv(path)
    df_name = df.select("Name")
    df_name.show()
    print(df_name.count())
    return df_name

def processData(path):

    df = sqlContext.read.parquet(path)
    df.show(5)
    print(df.count())

    df.registerTempTable('amazon_reviews')
    SQLQuery = 'SELECT review_body AS text FROM amazon_reviews'
    amazonReviews = sqlContext.sql(SQLQuery)

    # Get product titles
    #title = amazonReviews.groupBy("product_title").count().orderBy("product_title")
    #title.show(truncate=False)
    #print(title.count())

    #checkWords(title)

    return amazonReviews

def findEntities(df, show):

    # Load pre-trained model for recognize entities ==================================================
    pipeline = PipelineModel.load("./spark-nlp-models/explain_document_dl_en_2.1.0_2.4_1564764767733/")
    entities = pipeline.transform(amazonReviews)
    entities.registerTempTable('entities')
    
    SQLQuery = 'SELECT text, entities FROM entities'
    entities = sqlContext.sql(SQLQuery)
    
    if show == True:
        entities.printSchema()
        entities.show(truncate=False)
    return entities

def sentimentAnalysis(df, show):

    # Load pre-trained model for sentiment analysis ===================================================
    pipeline = PipelineModel.load("./spark-nlp-models/analyze_sentiment_en_2.1.0_2.4_1563204637489/")

    result = pipeline.transform(df)

    result.registerTempTable('result')
    SQLQuery = 'SELECT text, sentiment FROM result'
    sentiment = sqlContext.sql(SQLQuery)

    if show == True:
        sentiment.printSchema()
        sentiment.show()

    return sentiment

def checkWords(df):
    # Check if a word in a DaaFrame
    filterUDF = udf(intersect,BooleanType())
    df.where(filterUDF(df.product_title)).show(truncate=False)

def intersect(row):
    words = 'Bloodborne'
    # convert each word in lowecase
    #row = [x.lower() for x in row.split()]
    #return True if set(row).intersection(set(words)) else False
    return True if words in row else False

def main():
    path_VGNames = 's3a://insight-vgsales/vgsales-12-4-2019.csv'
    path_amazon_reviews = 's3a://insight-amazon-reviews/product_category=Video_Games/*.parquet'
    path_reddit_comments = 's3a://insight-reddit-comments/RC_2015-12.bz2'
    VGNames = readVGNames(path_VGNames)
    #df_amazonReviews = processData(path_amazon_reviews)
    
    SQLQuery = """
SELECT AR.product_title, VGN.Name, AR.text
FROM amazon_reviews AS AR
WHERE EXISTS (SELECT Name FROM VGNames AS VGN WHERE VGNames.Name IN amazon_reviews.product_title)
"""
    
    SQLQuery = """
SELECT product_title, product_body FROM amazon_reviews 
WHERE EXISTS (SELECT Name FROM VGNames AS VGN WHERE VGNames.Name IN amazon_reviews.product_title)
"""

    #result = sqlContext.sql(SQLQuery)
    #result.show()
#    df_sentiment = sentimentAnalysis(df, show = True)

if __name__ == "__main__":
    main()
