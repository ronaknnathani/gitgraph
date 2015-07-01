#!/usr/bin/env python

from pyspark import SparkContext
from pyspark.sql import SQLContext
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table

class watch(Model):
  reponame = columns.Text(primary_key=True)
  watchcount = columns.Integer()
  def __repr__(self):
    return '%s %d' % (self.reponame, self.watchcount)

connection.setup(['127.0.0.1'], "watch_events")
sc = SparkContext("spark://ip-172-31-2-89:7077", "watchevents")
sqlContext = SQLContext(sc)

df = sqlContext.jsonFile("hdfs://ec2-52-8-127-252.us-west-1.compute.amazonaws.com:9000/data_jan2015")
df_watch = df.filter("type='WatchEvent'")
df_commit = df.filter("type='CommitCommentEvent'")
df_fork = df.filter("type='ForkEvent'")
sqlContext.registerDataFrameAsTable(df_watch, "df_watch_table")
sqlContext.registerDataFrameAsTable(df_commit, "df_commit_table")
sqlContext.registerDataFrameAsTable(df_fork, "df_fork_table")

df_union = sqlContext.sql("SELECT * from df_watch_table UNION ALL SELECT * from df_commit_table UNION ALL SELECT * from df_fork_table")

mapcounts = df.map(lambda p: (p.repo.name[p.repo.name.find('/')+1:],1)).reduceByKey(lambda a,b: a+b)
collcounts = mapcounts.collect()

sync_table(watch)

for val in collcounts:
  watch.create(reponame=val[0], watchcount=val[1])
