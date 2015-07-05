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
class userrepo2014_1(Model):
  username = columns.Text(primary_key=True)
  repo = columns.List(columns.Text)
  def __repr__(self):
    return '%s %d' % (self.username, self.repo)

# getting master node's IP and public DNS to run Spark job and read from HDFS
master_ip = os.environ['master_ip']  
master_public_dns = os.environ['master_public_dns']

# setting SparkContext and SQLContext
sc = SparkContext("spark://" + master_ip + ":7077", "2014_events")
sqlContext = SQLContext(sc)

# reading events data for 2014 from HDFS
df14 = sqlContext.jsonFile("hdfs://" + master_public_dns + ":9000/data2014/2014-*.*")

# filtering rows with just the three relevant events
df14_watch = df14.filter("type='WatchEvent'")
df14_commit = df14.filter("type='CommitCommentEvent'")
df14_fork = df14.filter("type='ForkEvent'")

# registering  dataframes as tables to be able to select just the three relevant columns
sqlContext.registerDataFrameAsTable(df14_watch, "df14_watch_table")
sqlContext.registerDataFrameAsTable(df14_commit, "df14_commit_table")
sqlContext.registerDataFrameAsTable(df14_fork, "df14_fork_table")

# creating new dataframes with just the relevant columns
df14_watch_altered = sqlContext.sql("SELECT actor, repository, type FROM df14_watch_table")
df14_commit_altered = sqlContext.sql("SELECT actor, repository, type FROM df14_commit_table")
df14_fork_altered = sqlContext.sql("SELECT actor, repository, type FROM df14_fork_table")

# registering dataframes as tables to get a union of all
sqlContext.registerDataFrameAsTable(df14_watch_altered, "df14_watch_altered_table")
sqlContext.registerDataFrameAsTable(df14_commit_altered, "df14_commit_altered_table")
sqlContext.registerDataFrameAsTable(df14_fork_altered, "df14_fork_altered_table")

# unifying tables with filtered events and columns
df14_altered_union = sqlContext.sql("SELECT * from df14_watch_altered_table UNION ALL SELECT * from df14_commit_altered_table UNION ALL SELECT * from df14_fork_altered_table")

# functions to extract repo name as there are a few records with invalid fields     
def get_repo_name(x):
    try:
        return x.repository.owner+"/"+x.repository.name
    except Exception as e:
        print e, x

# grouping all records for a given username to get all repositories that the user is following and has contributed to
user_repo_map14 = df14_altered_union.map(lambda x: (x.actor, list([get_repo_name(x)]))).groupByKey()

# collecting the pipelined RDD as a list to be written to casandra table
user_repo14 = user_repo_map14.map(lambda x: {"username":x[0], "repo":[user for sublist in x[1] for user in sublist]}).collect()

# connecting to cassandra key space "watch_events" and syncing the desired table
connection.setup(['52.8.147.252','52.8.41.216'], "watch_events")
sync_table(userrepo2014)

# writing all values to cassandra table "userrepo2014"
for val in user_repo14:
    try:
        userrepo2014_1.create(username = val['username'], repo = val['repo'])
    except Exception as e:
        print e, val