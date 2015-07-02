#!/usr/bin/env python

from pyspark import SparkContext
from pyspark.sql import SQLContext
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
from cassandra.cluster import Cluster
from datetime import datetime

# model for "testuserrepo" in cassandra -> contains a table with 
# "username" as key and "repositories" contributed to/followed by user as value 
class testuserrepo(Model):
  username = columns.Text(primary_key=True)
  repo = columns.Map(columns.Text, columns.DateTime)
  def __repr__(self):
    return (self.username, self.repo)

# model for "testuserfollow" in cassandra -> contains a table with 
# "username" as key and a list of users followed by user as value  
class testuserfollow(Model):
  username = columns.Text(primary_key=True)
  f = columns.List(columns.Text)
  def __repr__(self):
    return (self.username, self.follows)

# connecting to "watch_events" keyspace in Cassandra and creating new Spark Context
connection.setup(['127.0.0.1', '52.8.41.216'], "watch_events")
sc = SparkContext("spark://ip-172-31-2-89:7077", "watchevents")
sqlContext = SQLContext(sc)

# reading data from HDFS and filtering with 'WatchEvent', 'CommitEvent', 'PushEvent'
df = sqlContext.jsonFile("hdfs://ec2-52-8-127-252.us-west-1.compute.amazonaws.com:9000/data_jan2015/2015-01-01-*")
df_watch = df.filter("type='WatchEvent'")
df_push = df.filter("type='PushEvent'")
df_commit = df.filter("type='CommitEvent'")

# registering data frames as tables to unite all rows
sqlContext.registerDataFrameAsTable(df_watch, "df_watch_table")
sqlContext.registerDataFrameAsTable(df_push, "df_push_table")
sqlContext.registerDataFrameAsTable(df_commit, "df_commit_table")
df_union = sqlContext.sql("SELECT * from df_watch_table UNION ALL SELECT * from df_push_table")

# spark job to group all repos for a given username considering the three filtered events above
userrepomap = df_union.map(lambda p: (p.actor.login, tuple((p.repo.name, datetime.strptime(p.created_at[:-1], "%Y-%m-%dT%H:%M:%S"))))).groupByKey()
userrepocoll = userrepomap.map(lambda x: (x[0], dict(x[1]))).collect()

# getting users->followers from HDFS
# reverse mapping it to users->following
dffoll = sqlContext.jsonFile("hdfs://ec2-52-8-127-252.us-west-1.compute.amazonaws.com:9000/user/2015-01-01-0_followers.json")
collfoll = dffoll.map(lambda x : (x.login, x.following)).collect()
follr = []

# follr is a list of dicts with users and people who these users follow
for v in collfoll:
    for f in v[1]:
        follr.append({"following": v[0], "user": f[1:]})

# creating a data frame from the list of dicts to run a spark job on
rdd = sc.parallelize(follr)
dffollr = sqlContext.createDataFrame(rdd)

# grouping username with people these users are following
# follr contains multiple dicts for a given username because of the reverse mapping from users->followers 
mapfollr = dffollr.map(lambda x: (x.user, tuple([x.following]))).groupByKey()
collfollr = mapfollr.map(lambda x: (x[0], list(x[1]))).collect()
collfollr = {x[0]: [y[0] for y in x[1]] for x in collfollr}

# sync Cassandra tables to insert data
sync_table(testuserrepo)
sync_table(testuserfollow)

# writing to tables in Cassandra
for val in userrepocoll:
  testuserrepo.create(username=val[0], repo=val[1])

for k,v in collfollr.iteritems():
  testuserfollow.create(username=k, f=v)

