#!/usr/bin/env python

# importing SparkContext and SQLContext from pyspark for batch processing
from pyspark import SparkContext
from pyspark.sql import SQLContext

# importing modules from cqlengine to write to cassandra table
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
import os

# defining schema to write to cassandra table
# schema defined should exactly match the table created in cassandra
class userrepo2011(Model):
  username = columns.Text(primary_key=True)
  repo = columns.List(columns.Text)
  def __repr__(self):
    return '%s %d' % (self.username, self.repo)

# getting master node's IP and public DNS to run Spark job and read from HDFS
master_ip = os.environ['master_ip']  
master_public_dns = os.environ['master_public_dns']

# setting SparkContext and SQLContext
sc = SparkContext("spark://" + master_ip + ":7077", "2015_events")
sqlContext = SQLContext(sc)

# reading events data for 2011 from HDFS
df11 = sqlContext.jsonFile(master_public_dns + ":9000/data_jan2015/2011-*.*")

# filtering rows with just the three relevant events
df11_watch = df11.filter("type='WatchEvent'")
df11_commit = df11.filter("type='CommitCommentEvent'")
df11_fork = df11.filter("type='ForkEvent'")

# registering  dataframes as tables to be able to select just the three relevant columns
sqlContext.registerDataFrameAsTable(df11_watch, "df11_watch_table")
sqlContext.registerDataFrameAsTable(df11_commit, "df11_commit_table")
sqlContext.registerDataFrameAsTable(df11_fork, "df11_fork_table")

# creating new dataframes with just the relevant columns
df11_watch_altered = sqlContext.sql("SELECT actor, repo, type FROM df11_watch_table")
df11_commit_altered = sqlContext.sql("SELECT actor, repo, type FROM df11_commit_table")
df11_fork_altered = sqlContext.sql("SELECT actor, repo, type FROM df11_fork_table")

# registering dataframes as tables to get a union of all
sqlContext.registerDataFrameAsTable(df11_watch_altered, "df11_watch_altered_table")
sqlContext.registerDataFrameAsTable(df11_commit_altered, "df11_commit_altered_table")
sqlContext.registerDataFrameAsTable(df11_fork_altered, "df11_fork_altered_table")

# unifying tables with filtered events and columns
df11_altered_union = sqlContext.sql("SELECT * from df11_watch_altered_table UNION ALL SELECT * from df11_commit_altered_table UNION ALL SELECT * from df11_fork_altered_table")

# grouping all records for a given username to get all repositories that the user is following and has contributed to
user_repo_map11 = df11_altered_union.map(lambda x: (x.actor.login, list([x.repo.name]))).groupByKey() 

# collecting the pipelined RDD as a list to be written to casandra table
user_repo11 = user_repo_map11.map(lambda x: {"username":x[0], "repo":[user for sublist in x[1] for user in sublist]}).collect()

# connecting to cassandra key space "watch_events" and syncing the desired table
connection.setup(['52.8.127.252','52.8.41.216'], "watch_events")
sync_table(userrepo2011)

# writing all values to cassandra table "userrepo2011"
for val in user_repo11:
  try:
    userrepo2011.create(username = val['username'], repo = val['repo'])
  except Exception as e:
    print e, val
