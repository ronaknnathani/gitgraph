#!/usr/bin/env python

from pyspark import SparkContext
from pyspark.sql import SQLContext
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table

class getuserid(Model):
  username = columns.Text(primary_key=True)
  userid = columns.Integer()
  def __repr__(self):
    return '%s %d' % (self.username, self.userid)

connection.setup(['127.0.0.1'], "watch_events")
sc = SparkContext("spark://ip-172-31-2-89:7077", "userid")
sqlContext = SQLContext(sc)
df = sqlContext.jsonFile("hdfs://ec2-52-8-127-252.us-west-1.compute.amazonaws.com:9000/camus/topics/github-usernames-good-1/hourly/2015/06/21/00/*")
names = df.map(lambda x: (x.login, x.id)).collect()

sync_table(getuserid)
for i, val in enumerate(names):
  getuserid.create(username=val[0], userid=val[1])
  print i
