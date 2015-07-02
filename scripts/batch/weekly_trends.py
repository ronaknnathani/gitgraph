#!/usr/bin/env python

# importing SparkContext and SQLContext from pyspark for batch processing
from pyspark import SparkContext
from pyspark.sql import SQLContext

# importing modules from cqlengine to write to cassandra table
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table

# defining schema to write to cassandra table
# schema defined should exactly match the table created in cassandra
class weeklytrends(Model):
  reponame = columns.Text(primary_key=True)
  watchcount = columns.Integer()
  def __repr__(self):
    return '%s %d' % (self.reponame, self.watchcount)

# getting master node's IP and public DNS to run Spark job and read from HDFS
master_ip = os.environ['master_ip']
master_public_dns = os.environ['master_public_dns']

# setting SparkContext and SQLContext
sc = SparkContext("spark://" + master_ip + ":7077", "weekly_trends")
sqlContext = SQLContext(sc)

# reading events for the last week from HDFS
df = sqlContext.jsonFile("hdfs://" + master_public_dns + ":9000/weekly_trends/")

# filtering watch events
df_watch = df.filter('type="WatchEvent"')

# counting numebr of stars for each repository in the data
watchcounts = df_watch.map(lambda p: (p.repo.name,1)).reduceByKey(lambda a,b: a+b).collect()

# connecting to cassandra key space "watch_events" and syncing the desired table
connection.setup(['52.8.41.216', '52.8.127.252'], "watch_events")
sync_table(weeklytrends)

# writing all values to cassandra table "weeklytrends"
for i, val in enumerate(watchcounts):
  weeklytrends.create(reponame=val[0], watchcount=val[1])
