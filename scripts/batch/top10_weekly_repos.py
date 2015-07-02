#!/usr/bin/env python

# importing modules from cqlengine to write to cassandra table
from cassandra.cluster import Cluster
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
import operator

# connecting to cassandra key space "watch_events" and syncing the desired table
connection.setup(['52.8.127.252', '52.8.41.216'], "watch_events")
cluster = Cluster(['52.8.127.252', '52.8.41.216'])
session = cluster.connect('watch_events')

# defining schema to write to cassandra table
# schema defined should exactly match the table created in cassandra
class top10weeklyrepos(Model):
  reponame = columns.Text(primary_key=True)
  watchcount = columns.Integer()
  def __repr__(self):
    return (self.reponame, self.watchcount)

# reading repos with watch counts from weeklytrends table
cql = "SELECT * FROM weeklytrends";
stmt = session.execute(cql)
response = []
for repo in stmt:
  response.append(repo)
jsonresponse = [(x.reponame,  x.watchcount) for x in response]

# sorting repos based on numbr of watch counts (stars)
jsonresponse.sort(key=operator.itemgetter(1), reverse=True)

# syncing the desired table to write to cassandra
sync_table(top10weeklyrepos)

# getting top 10 from the sorted list
top10 = jsonresponse[0:10]

# writing all values to cassandra table "top10weeklyrepos"
for val in top10:
    top10weeklyrepos.create(reponame = val[0], watchcount = val[1])

