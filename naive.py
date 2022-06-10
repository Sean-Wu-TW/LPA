def naive():
    d = {}
    with open('test.txt') as f:
        for line in f:
            for seg in line.split():
                if seg in d:
                    d[seg] += 1
                else:
                    d[seg] = 1

    with open('output.txt', 'w') as wf:
        i = 0
        for key, val in d.items():
            wf.write('{}, {} \n'.format(key, val))
            i += 1
            if i > 50: 
                break
def spark():
    import os
    os.environ["PYSPARK_PYTHON"] = "/Users/seanwu/opt/anaconda3/bin/python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/Users/seanwu/opt/anaconda3/bin/python"
    from pyspark.sql import SparkSession
    spark = SparkSession \
        .builder \
        .appName("Python Spark basic example") \
        .master("local[*]") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    distText = spark.sparkContext.textFile('test.txt')
    distText = distText.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda x, y:x+y)
    distText.saveAsTextFile('output')

if __name__ == '__main__':
    import boto3
    spark()
