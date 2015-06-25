#!/usr/bin/env python

from pyspark import SparkContext
from pyspark.sql import SQLContext
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
from cassandra.cluster import Cluster
from datetime import datetime

#cluster = Cluster(['52.8.127.252', '52.8.41.216'])
#session = cluster.connect('watch_events')

#class Repo(Object):
  #def __init__(self, reponame, datetime):
    #self.reponame = reponame
    #self.datetime = datetime

#class Follow(Object):
  #def __init__(self, following):
    #self.following = following

#cluster.register_user_type('watch_events', 'repo', Repo)
#cluster.register_user_type('watch_events', 'follow', Follow)

class testuserrepo(Model):
  username = columns.Text(primary_key=True)
  repo = columns.Map(columns.Text, columns.DateTime)
  def __repr__(self):
    return (self.username, self.repo)

class testuserfollow(Model):
  username = columns.Text(primary_key=True)
  f = columns.List(columns.Text)
  def __repr__(self):
    return (self.username, self.follows)

#cluster = Cluster(['52.8.127.252', '52.8.41.216'])
#session = cluster.connect('watch_events')

connection.setup(['127.0.0.1', '52.8.41.216'], "watch_events")
sc = SparkContext("spark://ip-172-31-2-89:7077", "watchevents")
sqlContext = SQLContext(sc)

# users with repos
df = sqlContext.jsonFile("hdfs://ec2-52-8-127-252.us-west-1.compute.amazonaws.com:9000/data_jan2015/2015-01-01-*")
df_watch = df.filter("type='WatchEvent'")
df_push = df.filter("type='PushEvent'")
df_commit = df.filter("type='CommitEvent'")
sqlContext.registerDataFrameAsTable(df_watch, "df_watch_table")
sqlContext.registerDataFrameAsTable(df_push, "df_push_table")
sqlContext.registerDataFrameAsTable(df_commit, "df_commit_table")
df_union = sqlContext.sql("SELECT * from df_watch_table UNION ALL SELECT * from df_push_table")
userrepomap = df_union.map(lambda p: (p.actor.login, tuple((p.repo.name, datetime.strptime(p.created_at[:-1], "%Y-%m-%dT%H:%M:%S"))))).groupByKey()
userrepocoll = userrepomap.map(lambda x: (x[0], dict(x[1]))).collect()

#users with following
dffoll = sqlContext.jsonFile("hdfs://ec2-52-8-127-252.us-west-1.compute.amazonaws.com:9000/user/2015-01-01-0_followers.json")
collfoll = dffoll.map(lambda x : (x.login, x.following)).collect()
follr = []
for v in collfoll:
    for f in v[1]:
        follr.append({"following": v[0], "user": f[1:]})

rdd = sc.parallelize(follr)
dffollr = sqlContext.createDataFrame(rdd)
mapfollr = dffollr.map(lambda x: (x.user, tuple([x.following]))).groupByKey()
collfollr = mapfollr.map(lambda x: (x[0], list(x[1]))).collect()
collfollr = {x[0]: [y[0] for y in x[1]] for x in collfollr}

sync_table(testuserrepo)
sync_table(testuserfollow)

for val in userrepocoll:
  testuserrepo.create(username=val[0], repo=val[1])

for k,v in collfollr.iteritems():
  testuserfollow.create(username=k, f=v)

