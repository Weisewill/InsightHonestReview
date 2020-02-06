# Import Spark NLP            
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.embeddings import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp
import pyspark
import pandas
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import map_values, expr, col, when, udf, lower, size
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import lit

# Configure spark SQL
conf = (SparkConf() \
        .setAppName("Process") \
        .set("spark.executor.instances", "1") \
        .set("spark.driver.cores", 6) \
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

def findName(df, names):
    # Add a new column "name"
    df = df.withColumn("name", lit("Null"))
    # Creates a DataFrame having a single column named "line"
    df = df.rdd.toDF(["product_title", "review_body", "word_count", "name"])
    df_line = df.select("product_title", "review_body", "word_count", "name")
    df_line = df_line.withColumn("product_title", f.lower(f.col("product_title")))

    for name in names:
        print("Finding name: " + name)
        df_line = df_line.withColumn("name", when(col("product_title").like("%"+name+"%"), name).otherwise(df_line.name))

    return df_line

def readData(path, cols, dataType):
    if dataType == "parquet":
        df = sqlContext.read.parquet(path)
    elif dataType == "CSV":
        df = sqlContext.read.option("header", "true").csv(path)
    df = df.select(cols)
    df.show()
    print(df.count())
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
        tmp2 = float(item2["confidence"]) if float(item2["confidence"]) >= 0.5 else 0
        score += tmp1 * tmp2
        count += 1
    return score / float(count)

def sentimentAnalysis(df, show):

    df.registerTempTable('amazon_reviews')
    SQLQuery = 'SELECT product_title, review_body, word_count, review_body AS text FROM amazon_reviews'
    amazonReviews = sqlContext.sql(SQLQuery)
    
    # Load pre-trained model for sentiment analysis ===================================================
    pipeline = PipelineModel.load("hdfs://10.0.0.12:9000/user/analyze_sentiment_en_2.1.0_2.4_1563204637489/")

    result = pipeline.transform(amazonReviews)

    result = result.selectExpr("product_title", "review_body", "word_count", "sentiment.result as result", "sentiment.metadata as confidence")    

    score = udf(lambda col1, col2: calculateScore(col1, col2), FloatType())

    df_sentiment = result.withColumn("score", score(result.result, result.confidence))

    if show == True:
        df_sentiment.printSchema()
        df_sentiment.show()

    return df_sentiment

def main():
    # Define path for the datasets
    path_VGNames = 's3a://insight-vgsales/vgsales-12-4-2019.csv'
    path_amazon_reviews = 's3a://insight-amazon-reviews/product_category=Video_Games/*.parquet'

    cols = ["Name", "word_count", "sentiment"]
    
    # Read in datasets
   # df_name = readData(path_VGNames, ["Name"], "CSV") 
    df_amazon = readData(path_amazon_reviews, ["product_title", "review_body"], "parquet")

   # Names = getNameList(df_name, 1000, 50)
   # names = [Name.lower() for Name in Names]
    Names = ["Metal Gear Solid", "Call of Duty"] 
    names = [Name.lower() for Name in Names]
    # Word count for every row 
    df_amazon = wordCount(df_amazon, "review_body")
    df_amazon.show()

    # Tag the names for the reviews
    df = findName(df_amazon, names)
    # Sentiment analysis
    df = sentimentAnalysis(df_amazon, True)

    #df.show(10)
    print("Finished!")

if __name__ == "__main__":
    main()
