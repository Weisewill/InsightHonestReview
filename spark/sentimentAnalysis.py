# Import Spark NLP           
import numpy as np
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.embeddings import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp
import pyspark
import pandas
import math
from pyspark.sql import SparkSession, Row
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql import DataFrameWriter
import boto3
sys.path.insert(1, '/home/ubuntu/config/')
from config import (AWS_ACCESS_ID, AWS_ACCESS_KEY, user, password, dbname)

# ===== AWS: create BOTO S3 client =====
s3 = boto3.client('s3',
                  aws_access_key_id=AWS_ACCESS_ID,
                  aws_secret_access_key=AWS_ACCESS_KEY)

# Configure spark SQL
conf = (SparkConf() \
        .setAppName("Process") \
        .set("spark.executor.instances", "1") \
        .set("spark.default.parallelism", "10")
        .set("spark.executor.memory", "12g"))
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName('Process data').getOrCreate()
spark = sparknlp.start()
spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")


def wordCount(df, colName):
    """
    Args:
        df: a DataFrame
        colName: a column for counting the number of words in it
    Returns:
        df: a DataFrame with one more column word_count of colName 
    """
    return df.withColumn('word_count', f.size(f.split(f.col(colName), ' ')))

def findName(df, names, table):
    """
    Steps:
        1. Add a new column called 'name' 
        2. Lower the whole text body of the column which are searched to label the product name
        3. Loop over names to find the product name asscociated to the review/comment 
    Args:
        df: a DataFrame
        names: a list of strings for the product names
        table: 'amazon_reviews' or 'reddit-comments' to specify which table to work on
    Returns:
        df: a DataFrame with one more column 'name' for the product name
    """
    df = df.withColumn("name", f.lit("Null"))
    if table == "amazon_reviews":
        df = df.rdd.toDF(["product_title", "review_body", "name"])
        df_line = df.select("product_title", "review_body", "name")
        df_line = df_line.withColumn("product_title", f.lower(f.col("product_title")))
        colName = "product_title"
    elif table == "reddit_comments":
        df = df.rdd.toDF(["body", "name"])
        df_line = df.select("body", "name")
        df_line = df_line.withColumn("body", f.lower(f.col("body")))
        colName = "body"
    for name in names:
        print("Finding name: " + name)
        df_line = df_line.withColumn("name", f.when(f.col(colName).like("%"+name+"%"), name).otherwise(df_line.name))
    df_line = df_line.filter(df_line.name != "Null")
    return df_line

def readData(path, cols, dataType):
    """
    Read data from sources.
    Args:
        path: path to the source
        cols: columns to select
        dataType: data type of the source, CSV or parquet
    Returns:
        df: a DataFrame
    """
    
    if dataType == "parquet":
        df = sqlContext.read.parquet(path)
    elif dataType == "CSV":
        df = sqlContext.read\
                .format('csv')\
                .option('header', 'true')\
                .option('mode', 'DROPMALFORMED')\
                .option('inferSchema', 'true')\
                .load(path)
    df = df.select(cols)
    return df

def getNameList(df, num, length, VGStartRank):
    """
    Steps:
        1. Get the first num popular games
        2. Sort the list by string length of the Names in ascending order
    Args: 
        df: input DataFrame
        num: # of name to pick
        length: length limit for the names
        VGStartRank: Starting rank for video games.
    Return:
        A list of names
    """
    df_pandas = df.where( f.length("Name")  <= length ).toPandas()[:num]
    df_pandas = df_pandas.drop_duplicates(['Name'])
    idx = df_pandas.Name.str.len().sort_values().index
    df_pandas = df_pandas.reindex(idx)
    print(df_pandas)
    return [row for row in df_pandas.Name]

def calculateScore(col1, col2):
    """
    Calculate the sentiment analysis score
    score = p - n
    p - n = + ln(confidence) if "positive"
          = - ln(confidence) if "negative"

    Args:
        col1: a string could be 'positive' or 'negative' sentiment
        col2: a string for the confidence of the sentiment, need to convert to float

    Returns:
        Score

    """
    score = 0
    count = 0
    for item1, item2 in zip(col1, col2):
        sign = -1.0 if item1 == "negative" else 1.0
        tmp = float(item2['confidence'])
        tmp = tmp*tmp*tmp*tmp
        score += sign*tmp
        count += 1

    return score / float(count) if count > 0 else 0.0

def sentimentAnalysis(df, show, table):
    """
    Perform sentiment analysis
    Steps:
        1. Load pre-trained NLP model pipeline
        2. Transform the text body and create a new column called sentiment
    Args:
        df: input DataFrame
        show: option for showing df_entiment
        table: 'amazon_reviews' or 'reddit-comments' to specify which table to work on
    Returns:
        df_sentiment: a DataFrame with sentiment
    """
    if table == "amazon_reviews":
        colName = "review_body"
    elif table == "reddit_comments":
        colName = "body"

    df = df.withColumn("text", f.col(colName))
    df = df.na.drop()
    #df.show()

    # Load pre-trained model for sentiment analysis ===================================================
    #pipeline = PipelineModel.load("hdfs://10.0.0.12:9000/user/analyze_sentiment_en_2.1.0_2.4_1563204637489/")
    pipeline = PipelineModel.load("hdfs://10.0.0.12:9000/user/analyze_sentiment_en_2.4.0_2.4_1580483464667/")
    result = pipeline.transform(df)
    
    ### start test ###
    #result = pipeline.annotate(df, "text")
    #result.printSchema()
    #result.show()
    ### end test ###

    result = result.selectExpr(colName,"name", "word_count", "sentiment.result AS result", "sentiment.metadata AS confidence")
    
    score = f.udf(lambda col1, col2: calculateScore(col1, col2), FloatType())
    
    df_sentiment = result.withColumn("score", score( result.result, result.confidence ))

    if show == True:
        df_sentiment.printSchema()
        df_sentiment.show()
    
    # Select relavent columns
    df_sentiment = df_sentiment.selectExpr(colName, "name", "word_count", "score")
    
    #df_sentiment = df_sentiment.select(when(df_sentiment.score.isNull(), 0 ).otherwise(df_sentiment.score).alias("score"))
    #df_sentiment.printSchema()
    #df_sentiment.show(10)
    print("Finish sentiment analysis...")
    
    #print(df_sentiment.count())
    return df_sentiment

def saveToDB(df, mode, table):
    """
    Save DataFrame to SQL database
    Args:
        df: DataFrame to save
        mode: option for saving the df 
            'append' Contents of this SparkDataFrame are expected to be appended to existing data.
            'overwrite' Existing data is expected to be overwritten by the contents of this SparkDataFrame.
            'error' An exception is expected to be thrown.
            'ignore' The save operation is expected to not save the contents of the SparkDataFrame and to not change the existing data.
        table: 'amazon_reviews' or 'reddit-comments' to specify which table to work on
    """
    print("Save data to postgres.")
    url = "jdbc:postgresql://ec2-44-231-209-180.us-west-2.compute.amazonaws.com:5432/{}".format(dbname)
    properties = {"user": user,"password": password,"driver": "org.postgresql.Driver"}
    #df = save_to_hdfs_first("id", df)
    #data = DataFrameWriter(df) 
    #data.option("batchsize", 10000000).jdbc(url=url, table="amazon_reviews", mode=mode, properties=properties)
    #df.printSchema()
    #df.show(truncate=False)
    #df = df.repartition(20)
    df.write.jdbc(url=url, table=table, mode=mode, properties=properties)

def main(table, mode, VGStartRank, reddit_year, reddit_month):
    """
    Process on Spark
    Steps:
        1. Define path for the datasets
        2. Read in names datasets
        3. Tag the names for the reviews
        4. Find word count for every review
        5. Perform sentiment analysis
        6. Save result to PostgreSQL
    Args:
        table: input from args, can be 'amazon' or 'reddit'
        mode: see description in saveToDB()
        VGStartRank: Starting rank for video game.
        reddit_year: for reddit only, specify which year to work on
        reddit_month: for reddit only, specify which month to work on
    """
    path_VGNames = 's3a://insight-vgsales/vgsales-12-4-2019.csv'
    path_amazon_reviews = 's3a://insight-amazon-reviews/product_category=Video_Games/*.parquet'
    path_reddit_comments = 's3a://insight-reddit-comments-raw/{}/*{}.parquet' \
        .format(reddit_year, "0"+str(reddit_month) if reddit_month < 10 else str(reddit_month))
    cols = ["Name", "word_count", "sentiment"]

    # Read in names datasets
    df_name = readData(path_VGNames, ["Name"], "CSV")
    Names = getNameList(df_name, 100, 30, int(VGStartRank))
    names = [Name.lower() for Name in Names]
    # Some test names
    #Names = ["The Legend of Zelda", "Bloodborne"] 
    #names = [Name.lower() for Name in Names]

    if table == "amazon":
        df_amazon = readData(path_amazon_reviews, ["product_title", "review_body"], "parquet")
        print("Processing amazon reviews data...")
        df_proc = findName(df_amazon, names, "amazon_reviews")
        df_proc = wordCount(df_proc, "review_body")
        df_proc = sentimentAnalysis(df_proc, show = False, table = "amazon_reviews")
        saveToDB(df_proc, mode = mode, table = "amazon_reviews")

    elif table == 'reddit':
        df_reddit = readData(path_reddit_comments, ["body"], "parquet")
        print("Processing reddit comments data...")
        df_proc = findName(df_reddit, names, "reddit_comments")
        df_proc = wordCount(df_proc, "body")
        df_proc = sentimentAnalysis(df_proc, show = False, table = "reddit_comments")
        saveToDB(df_proc, mode = mode, table = "reddit_comments")

if __name__ == "__main__":
    table = sys.argv[1]
    try:
        mode = sys.argv[2]
    except:
        mode = 'append'
        print('Using default mode: append for writting to database.')
    try:
        VGStartRank = int(sys.argv[3])
    except:
        VGStartRank = 0
        print("Please enter starting rank for video game name.")
    try:
        reddit_year = int(sys.argv[4])
    except:
        reddit_year = 2014
        print("Please enter year if you want to process reddit review! Using default year 2014.")
    try:
        reddit_month = int(sys.argv[5])
    except:
        reddit_month = 1
        print("Please enter month if you want to process reddit review! Using default month 1.")
    
    main(table, mode, VGStartRank, reddit_year, reddit_month)
    print("Finished...!")
