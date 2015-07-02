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
import json

# defining schema to write to cassandra table
# schema defined should exactly match the table created in cassandra
class userfollow(Model):
  username = columns.Text(primary_key=True)
  following = columns.List(columns.Text)
  def __repr__(self):
    return '%s %d' % (self.username, self.following)

# getting master node's IP and public DNS to run Spark job and read from HDFS
master_ip = os.environ['master_ip']
master_public_dns = os.environ['master_public_dns']

# setting SparkContext and SQLContext
sc = SparkContext("spark://" + master_ip + ":7077", "user_following")
sqlContext = SQLContext(sc)

# reading users->following records from multiple topics from HDFS
df_topic3 = sqlContext.jsonFile("hdfs://" + master_public_dns + ":9000/camus/topics/scrap-following-3/hourly/2015/*/*/*/*")
df_topic4 = sqlContext.jsonFile("hdfs://" + master_public_dns + ":9000/camus/topics/scrap-following-4/hourly/2015/*/*/*/*")
df_topic5 = sqlContext.jsonFile("hdfs://" + master_public_dns + ":9000/camus/topics/scrap-following-5/hourly/2015/*/*/*/*")
df_topic7 = sqlContext.jsonFile("hdfs://" + master_public_dns + ":9000/camus/topics/scrap-following-7/hourly/2015/*/*/*/*")

# function to parse dataframe row and return a list of users followed by a user in that given row of the dataframe
# for topics- 3, 4, 5 the nested json for the list of users that a given user is following is in unicode. So, loading using json, making it a dict and returning the list
def follows(x): 
  try:
    following_list = []
    if x == []:
      return following_list
    else:
      for user in x:
        following_list.append(json.loads(user)['login'])
      return following_list
  except Exception as e:
    print x, e

df_topic3_following = df_topic3.map(lambda x: (x.login, follows(x.following))).collect()
df_topic4_following = df_topic4.map(lambda x: (x.login, follows(x.following))).collect()
df_topic5_following = df_topic5.map(lambda x: (x.login, follows(x.following))).collect()

# topic 7 has a different schema than topics 3, 4, 5. Hence, a separate function to get the list of users that a given user is following
# for opic 7 the nested json for the list of users that a given user is following is saved as a dataframe
def follows7(x):
  following = []
  try:
    if x==[]:
      return following
    else:
      for user in x:
        following.append(user.login)
      return following
  except Exception as e:
    print x, e

df_topic7_following = df_topic7.map(lambda x: (x.login, follows7(x.following))).collect()

# connecting to cassandra key space "watch_events" and syncing the desired table
connection.setup(['52.8.127.252','52.8.41.216'], "watch_events")
sync_table(userfollow)

# writing all values to cassandra table "userfollow"
for val in df_topic3_following:
  userfollow.create(username = val[0], following = val[1])

for val in df_topic4_following:
  userfollow.create(username = val[0], following = val[1])

for val in df_topic5_following:
  userfollow.create(username = val[0], following = val[1])

for val in df_topic7_following:
  userfollow.create(username = val[0], following = val[1])

