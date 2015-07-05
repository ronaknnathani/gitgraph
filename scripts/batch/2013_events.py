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
class userrepo2013(Model):
  username = columns.Text(primary_key=True)
  repo = columns.List(columns.Text)
  def __repr__(self):
    return '%s %d' % (self.username, self.repo)

# getting master node's IP and public DNS to run Spark job and read from HDFS
master_ip = os.environ['master_ip']  
master_public_dns = os.environ['master_public_dns']

# setting SparkContext and SQLContext
sc = SparkContext("spark://" + master_ip + ":7077", "2013_events")
sqlContext = SQLContext(sc)

# reading events data for 2013 from HDFS
df13 = sqlContext.jsonFile("hdfs://" + master_public_dns + ":9000/data_jan2015/2013-*.*")

# filtering rows with just the three relevant events
df13_watch = df13.filter("type='WatchEvent'")
df13_commit = df13.filter("type='CommitCommentEvent'")
df13_fork = df13.filter("type='ForkEvent'")

# registering  dataframes as tables to be able to select just the three relevant columns
sqlContext.registerDataFrameAsTable(df13_watch, "df13_watch_table")
sqlContext.registerDataFrameAsTable(df13_commit, "df13_commit_table")
sqlContext.registerDataFrameAsTable(df13_fork, "df13_fork_table")

# creating new dataframes with just the relevant columns
df13_watch_altered = sqlContext.sql("SELECT actor, repository, type FROM df13_watch_table")
df13_commit_altered = sqlContext.sql("SELECT actor, repository, type FROM df13_commit_table")
df13_fork_altered = sqlContext.sql("SELECT actor, repository, type FROM df13_fork_table")

# registering dataframes as tables to get a union of all
sqlContext.registerDataFrameAsTable(df13_watch_altered, "df13_watch_altered_table")
sqlContext.registerDataFrameAsTable(df13_commit_altered, "df13_commit_altered_table")
sqlContext.registerDataFrameAsTable(df13_fork_altered, "df13_fork_altered_table")

# unifying tables with filtered events and columns
df13_altered_union = sqlContext.sql("SELECT * from df13_watch_altered_table UNION ALL SELECT * from df13_commit_altered_table UNION ALL SELECT * from df13_fork_altered_table")

# functions to extract repo name as there are a few records with invalid fields     
def get_repo_name(x):
    try:
        return x.repository.owner+"/"+x.repository.name
    except Exception as e:
        print e, x

# grouping all records for a given username to get all repositories that the user is following and has contributed to
user_repo_map13 = df13_altered_union.map(lambda x: (x.actor, list([get_repo_name(x)]))).groupByKey()

# collecting the pipelined RDD as a list to be written to casandra table
user_repo13 = user_repo_map13.map(lambda x: {"username":x[0], "repo":[user for sublist in x[1] for user in sublist]}).collect()

# connecting to cassandra key space "watch_events" and syncing the desired table
connection.setup(['52.8.137.252','52.8.41.216'], "watch_events")
sync_table(userrepo2013)

# writing all values to cassandra table "userrepo2013"
for val in user_repo13:
    try:
        userrepo2013.create(username = val['username'], repo = val['repo'])
    except Exception as e:
        print e, val