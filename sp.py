'''

To extract one single file from
a wat file

'''


import os
import pyspark
from pyspark.sql import SparkSession
import configparser
import argparse

def main(sparkSession):
	pass



if __name__ == '__main__':
	spark = SparkSession \
    .builder \
    .appName("Python Spark basic example") \
    .master("local[*]") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

    file = spark.sparkContext.textFile('bbc.pretty.wat.txt')
    print()







