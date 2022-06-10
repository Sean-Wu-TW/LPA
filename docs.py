## for script only- specify python version##
import os
os.environ["PYSPARK_PYTHON"] = "/Users/seanwu/opt/anaconda3/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/Users/seanwu/opt/anaconda3/bin/python"

### config ###
import pyspark
conf = pyspark.SparkConf().setAppName("appName").setMaster("local[*]")
sc = pyspark.SparkContext()

### session, on spark 2.0, all configs, contexts are bundled to session ###
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark basic example") \
    .master("local[*]") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


### for aws config ###
import configparser
aws_profile = "default"
config = configparser.ConfigParser()
config.read(os.path.expanduser("~/.aws/credentials"))
access_id = config.get(aws_profile, "aws_access_key_id") 
access_key = config.get(aws_profile, "aws_secret_access_key")


### config hadoop ###
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.awsAccessKeyId", access_id)
hadoop_conf.set("fs.s3a.awsSecretAccessKey", access_key)
hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")

### not working!!?? ###
# distFile = spark.SparkContext.textFile('/Users/seanwu/Desktop/learn/spark/February 2016/crawl-data/CC-MAIN-2016-07/segments/1454702039825.90/wat/*.gz')

distFile = sc.textFile("s3a://crawl-data/CC-MAIN-2017-22/segments/1495463605188.47/wet/CC-MAIN-20170522151715-20170522171715-00000.warc.wet.gz")

# distFile = spark.textFile("bible.txt")
# distFile = sc.textFile("s3a://test4sean/test/bible.txt")
distFile = sc.textFile("/Users/seanwu/Desktop/learn/spark/February 2016/crawl-data/CC-MAIN-2016-07/segments/1454702039825.90/wat/*.gz")
print(distFile.count())
# counts = distFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
# counts.saveAsTextFile("gggg")


### text graphFrame ###
from functools import reduce
from pyspark.sql.functions import col, lit, when
from graphframes import *

vertices = sqlContext.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36),
  ("g", "Gabby", 60)], ["id", "name", "age"])

edges = sqlContext.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend")
], ["src", "dst", "relationship"])

g = GraphFrame(vertices, edges)
print(g)





