#!/usr/bin/env python

from pyspark import SparkContext
from pyspark.sql import SQLContext
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table

class testuserrepo(Model):
  username = columns.Text(primary_key=True)
  repos = columns.List<Text>()
  def __repr__(self):
    return '%s %d' % (self.username, self.repos)

class testuserfollow(Model):
  username = columns.Text(primary_key=True)
  follow = columns.List<Text>()
  def __repr__(self):
    return '%s %d' % (self.username, self.follow)


connection.setup(['127.0.0.1', '52.8.41.216'], "watch_events")
sc = SparkContext("spark://ip-172-31-2-89:7077", "watchevents")
sqlContext = SQLContext(sc)

# users with repos
df = sqlContext.jsonFile("hdfs://ec2-52-8-127-252.us-west-1.compute.amazonaws.com:9000/data_jan2015/2015-01-01-*")
df_watch = df.filter("type='WatchEvent'")
df_push = df.filter("type='PushEvent'")
df_commit = df.filter("type='CommitEvent'")
df_union = sqlContext.sql("SELECT * from df_watch_table UNION ALL SELECT * from df_push_table")
userrepomap = df_union.map(lambda p: (p.actor.login, list((p.repo.name, p.created_at)))).groupByKey()
userrepocoll = userrepomap.map(lambda x: (x[0], list(x[1]))).collect()

#users with following
dffoll = sqlContext.jsonFile("hdfs://ec2-52-8-127-252.us-west-1.compute.amazonaws.com:9000/user/2015-01-01-0_followers.json")
collfoll = dffoll.map(lambda x : (x.login, x.following)).collect()
follr = []
for v in collfoll:
    for f in v[1]:
        follr.append({"following": v[0], "user": f})

rdd = sc.parallelize(follr)
dffollr = sqlContext.createDataFrame(rdd)
mapfollr = dffollr.map(lambda x: (x.user, list([x.following]))).groupByKey()
collfollr = mapfollr.map(lambda x: (x[0], list(x[1]))).collect()

sync_table(testuserrepo)
sync_table(testuserfollow)

for val in userrepocoll:
  testuserrepo.create(username=val[0], repos=val[1])

for val in collfollr:
  testuserfollow.create(username=val[0], follow=val[1])

