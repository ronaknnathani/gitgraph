# This script saves all the 12M+ usernames and their respective IDs collected using GitHub's API 
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
class getuserid(Model):
  username = columns.Text(primary_key=True)
  userid = columns.Integer()
  def __repr__(self):
    return '%s %d' % (self.username, self.userid)

# connecting to cassandra key space "watch_events" and syncing the desired table
connection.setup(['127.0.0.1'], "watch_events")

# getting master node's IP and public DNS to run Spark job and read from HDFS
master_ip = os.environ['master_ip']
master_public_dns = os.environ['master_public_dns']

# setting SparkContext and SQLContext
sc = SparkContext("spark://" + master_ip + ":7077", "userid")
sqlContext = SQLContext(sc)

# reading data for collected usernames
df = sqlContext.jsonFile("hdfs://" + master_public_dns + ":9000/camus/topics/github-usernames-good-1/hourly/2015/06/21/00/*")

# Spark job to get just the login names and IDs from all the fields in the data
names = df.map(lambda x: (x.login, x.id)).collect()

# sync cassandra table getuserid
sync_table(getuserid)

# writing results to cassandra table
for i, val in enumerate(names):
  getuserid.create(username=val[0], userid=val[1])
  print i
