#!/usr/bin/env python

from pyspark import SparkContext
from pyspark.sql import SQLContext
import pickle
#from cqlengine import columns
#from cqlengine.models import Model
#from cqlengine import connection
#from cqlengine.management import sync_table

#class watch(Model):
#  reponame = columns.Text(primary_key=True)
#  watchcount = columns.Integer()
#  def __repr__(self):
#    return '%s %d' % (self.reponame, self.watchcount)

#connection.setup(['127.0.0.1'], "watch_events")
sc = SparkContext("spark://ip-172-31-2-89:7077", "watchpush")
sqlContext = SQLContext(sc)

df = sqlContext.jsonFile("hdfs://ec2-52-8-127-252.us-west-1.compute.amazonaws.com:9000/data_jan2015/2015-01-01-0.json")
df_watch = df.filter("type='WatchEvent'")
df_push = df.filter("type='PushEvent'")
sqlContext.registerDataFrameAsTable(df_watch, "df_watch_table")
sqlContext.registerDataFrameAsTable(df_push, "df_push_table")
df_union = sqlContext.sql("SELECT * from df_watch_table UNION ALL SELECT * from df_push_table")
userrepomap = df_union.map(lambda p: (p.actor.login, list((p.repo.name, p.created_at)))).groupByKey()
userrepocoll = userrepomap.map(lambda x: (x[0], list(x[1]))).collect()

usernames = [v[0] for v in userrepocoll]
print len(usernames)
f = open("usernames","wb")
pickle.dump(usernames, f)
f.close()
#sync_table(watch)

#for val in collcounts:
#  watch.create(reponame=val[0], watchcount=val[1])
