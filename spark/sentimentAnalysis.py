# Import Spark NLP            
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.embeddings import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp
import pyspark
import pandas
from pyspark.sql import SparkSession, Row
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import map_values, expr, col, when, udf, lower, size
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import lit
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
        .set("spark.executor.instances", "4") \
        .set("spark.driver.memory", "50g") 
        .set("spark.executor.memory", "6g"))
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName('Reddit Comments ETL').getOrCreate()
spark = sparknlp.start()
spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")


def addColumnToDF(df, cols, idx, newType):
    # newType : type for the new column, i for int or c for char
    if newType == "i":
        df = df.withColumn(cols[idx], lit(0))
    elif newType == "c":
        df = df.withColumn(cols[idx], lit("Null")) 
    return df

def wordCount(df, colName):
    # return the word count of a column
    return df.withColumn('word_count', f.size(f.split(f.col(colName), ' ')))

def findName(df, names, table):
    # Add a new column "name"
    df = df.withColumn("name", lit("Null"))
    if table == "amazon_reviews":
        # Creates a DataFrame having a single column named "line"
        df = df.rdd.toDF(["product_title", "review_body", "name"])
        df_line = df.select("product_title", "review_body", "name")
        df_line = df_line.withColumn("product_title", f.lower(f.col("product_title")))
        colName = "product_title"
    elif table == "reddit_comments":
        # Creates a DataFrame having a single column named "line"
        df = df.rdd.toDF(["body", "name"])
        df_line = df.select("body", "name")
        df_line = df_line.withColumn("body", f.lower(f.col("body")))
        colName = "body"
    # Find the names in the commente/reviews
    for name in names:
        print("Finding name: " + name)
        df_line = df_line.withColumn("name", when(col(colName).like("%"+name+"%"), name).otherwise(df_line.name))
    #name = names
    #print("Finding name: " + name)
    #df_line = df_line.withColumn("name", when(col("product_title").like("%"+name+"%"), name).otherwise(df_line.name))
    # Only select those with names
    df_line = df_line.filter(df_line.name != "Null")
    return df_line

def readData(path, cols, dataType):
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
    #df.show()
    #print(df.count())
    return df

def getNameList(df, num, length):
    # Get the first num popular games
    # Sorted by Name string length
    df_pandas = df.where( f.length("Name")  <= length ).toPandas()[0:num]
    idx = df_pandas.Name.str.len().sort_values().index
    df_pandas = df_pandas.reindex(idx)
    print(df_pandas)
    return [row for row in df_pandas.Name]

def calculateScore(col1, col2):
    score = 0
    count = 0
    for item1, item2 in zip(col1, col2):
        tmp1 = -1 if item1 == "negative" else 1
        #if float(item2["confidence"]) >= 0.6:
        #    tmp2 = float(item2["confidence"])
        #elif float(item2["confidence"]) >= 0.4:
        #    tmp2 = float(item2["confidence"]) / 2.0
        #else:
        #    tmp2 = float(item2["confidence"]) / 3.0
        tmp2 = float(item2["confidence"]) if float(item2["confidence"]) >= 0.6 else 0.0
        if tmp2 > 0.0:
            score += tmp1 * tmp2
            count += 1
    return score / float(count) if count > 0 else 0.0

def sentimentAnalysis(df, show, table):

    if table == "amazon_reviews":
        colName = "review_body"
    elif table == "reddit_comments":
        colName = "body"

    df = df.withColumn("text", col(colName))
    df = df.na.drop()
    #df.show()

    # Load pre-trained model for sentiment analysis ===================================================
    #pipeline = PipelineModel.load("hdfs://10.0.0.12:9000/user/analyze_sentiment_en_2.1.0_2.4_1563204637489/")
    pipeline = PipelineModel.load("hdfs://10.0.0.12:9000/user/analyze_sentiment_en_2.4.0_2.4_1580483464667/")
    result = pipeline.transform(df)

    result = result.selectExpr(colName, "name", "word_count", "sentiment.result AS result", "sentiment.metadata AS confidence")

    score = udf(lambda col1, col2: calculateScore(col1, col2), FloatType())
    
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
    mode options 
    append: Contents of this SparkDataFrame are expected to be appended to existing data.
    overwrite: Existing data is expected to be overwritten by the contents of this SparkDataFrame.
    error: An exception is expected to be thrown.
    ignore: The save operation is expected to not save the contents of the SparkDataFrame and to not change the existing data.
    """
    print("Save data to postgres.")
    url = "jdbc:postgresql://ec2-44-231-209-180.us-west-2.compute.amazonaws.com:5432/{}".format(dbname)
    properties = {"user": user,"password": password,"driver": "org.postgresql.Driver"}
    #df = save_to_hdfs_first("id", df)
    #data = DataFrameWriter(df) 
    #data.option("batchsize", 10000000).jdbc(url=url, table="amazon_reviews", mode=mode, properties=properties)
    #df.printSchema()
    #df.show(truncate=False)
    df = df.repartition(100)
    df.write.jdbc(url=url, table=table, mode=mode, properties=properties)

def main(table, reddit_year):
    # Define path for the datasets
    path_VGNames = 's3a://insight-vgsales/vgsales-12-4-2019.csv'
    path_amazon_reviews = 's3a://insight-amazon-reviews/product_category=Video_Games/*.parquet'
    path_reddit_comments = 's3a://insight-reddit-comments-raw/{}/*.parquet'.format(reddit_year)
    cols = ["Name", "word_count", "sentiment"]

    # Read in names datasets
    df_name = readData(path_VGNames, ["Name"], "CSV")
    Names = getNameList(df_name, 10, 30)
    names = [Name.lower() for Name in Names]
    # Some test names
    #Names = ["The Legend of Zelda", "Bloodborne"] 
    #names = [Name.lower() for Name in Names]

    if table == "amazon":
        df_amazon = readData(path_amazon_reviews, ["product_title", "review_body"], "parquet")
        print("Processing amazon reviews data...")
        # Tag the names for the reviews
        df_proc = findName(df_amazon, names, "amazon_reviews")
        # Word count for every row
        df_proc = wordCount(df_proc, "review_body")
        # Sentiment analysis
        df_proc = sentimentAnalysis(df_proc, show = False, table = "amazon_reviews")
        # Save to PostgreSQL
        saveToDB(df_proc, mode = "overwrite", table = "amazon_reviews")

    elif table == 'reddit':
        df_reddit = readData(path_reddit_comments, ["body"], "parquet")
        print("Processing reddit comments data...")
        # Tag the names for the reviews
        df_proc = findName(df_reddit, names, "reddit_comments")
        # Word count for every row
        df_proc = wordCount(df_proc, "body")
        # Sentiment analysis
        df_proc = sentimentAnalysis(df_proc, show = False, table = "reddit_comments")
        # Save to PostgreSQL
        saveToDB(df_proc, mode = "overwrite", table = "reddit_comments")

if __name__ == "__main__":
    table = sys.argv[1]
    try:
        reddit_year = int(sys.argv[2])
    except:
        print("Please enter year for reddit review!")
    main(table, reddit_year)
    print("Finished...!")
