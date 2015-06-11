from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext("spark://ip-172-31-2-89:7077", "watchevents")
sqlContext = SQLContext(sc)

df = sqlContext.jsonFile("hdfs://ec2-52-8-127-252.us-west-1.compute.amazonaws.com:9000/data_jan2015/2015-01-01-0.json")
df = df.filter("type='WatchEvent'")
mapcounts = df.map(lambda p: (p.repo.name,1)).reduceByKey(lambda a,b: a+b)
collcounts = mapcounts.collect()
for val in collcounts:
  print val[find('/') + 1 :]

#collcounts.saveAsTextFile("hdfs://ec2-52-8-127-252.us-west-1.compute.amazonaws.com:9000/watch_op")
