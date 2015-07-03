#!/usr/bin/env python

# importing flask and cassandra modules
from cassandra.cluster import Cluster
from flask import Flask, jsonify, render_template, request
import json
from cassandra.query import SimpleStatement
import operator
from sets import Set

# importing script to fetch data from API in real time
import fetch_data

# setting up connections to cassandra
cluster = Cluster(['52.8.127.252', '52.8.41.216'])
session = cluster.connect('watch_events')

app = Flask(__name__)

# homepage
@app.route("/")
@app.route("/index")
def hello():
  return render_template("index.html")

# trending repositories in the last week based on number of stars 
@app.route("/weeklytrends")
def weekly_trends():
  cql = "SELECT * FROM top10weeklyrepos"; # querying cassandra to fetch the data
  stmt = session.execute(cql) # executing the query
  response = []
  for repo in stmt:
    response.append(repo) # getting the returned rows in a list
  responselist = [(x.reponame,  x.watchcount) for x in response] # taking the reponames and corresponding watchcount
  responselist.sort(key=operator.itemgetter(1), reverse = True) # sorting the repos based on watchcounts in descendng order
  jsonresponse = [{"reponame":x[0], "watchcount":x[1]} for x in responselist] # creating a json response
  return render_template("weeklytrends.html", trends = responselist) # returning the josnresponse to the template

# get user id page
@app.route("/userid")
def userid():
  return render_template("userid.html")

# receive username and retrun id
@app.route("/userid", methods=['POST'])
def userid_post():
  username = request.form["userid"] # getting the username entered by the user
  cql = "SELECT userid FROM getuserid WHERE username=%s" # querying cassandra to fetch the data
  stmt = session.execute(cql, parameters=[username]) # executing the query
  if stmt!=[]:  # taking care of non-existent usernames/ usernames not in the database
    jsonresponse = {"userid": stmt[0].userid} # creating a json response
    return render_template("useridresult.html", user_id = jsonresponse) # returning the josnresponse to the template
  else: 
    jsonresponse = {"userid": username + " is not in the database"} # creating a json response if the username doesn't exist
    return render_template("no_userid.html", user_id = jsonresponse) # rendering template with the response

# link to the slides
@app.route("/slides")
def slides():
  return render_template("slides.html")
  
# graph query page
@app.route("/graph")
def graph():
  return render_template("graph_query.html")

# get username and return graph
@app.route("/graph", methods=['POST'])
def graph_post():
  username = request.form["username"] # get username entered
  cql = "SELECT following from userfollow WHERE username=%s" # build the query
  try:
    stmt = session.execute(cql, parameters=[username]) # execute the query
    if stmt==[]: # if the user details are not in the database, fetch from API
      get_graph(username) # get user details
      stmt = session.execute(cql, parameters=[username]) # execute the query after getting details from API
    following_list = stmt[0].following # get the list of people followed by the user
    if following_list==None: # if user doesn't follow anyone, return the corresponding response
      return render_template("nofollowing.html", response=username)
    repojson = []
    for all in following_list: # for all users that the given user follows get the respective repos
      cql = "SELECT repo from userrepo WHERE username=%s"
      reporow = session.execute(cql, parameters=[all])
      if reporow==[]:
        repojson.append({"name": all}) # if there are no repos the there are no children to the node, just people followed by the user
      else:        
        repojson.append({"name": all, "children": [{"name":reponame, "size":10000} for reponame in Set(list(reporow[0].repo))]}) # if there are repos that the user follows/contributes to, create the nested json for tree
    jsonresponse = {"name": username, "children": [x for x in repojson]} # final json with parent and children for the tree
    return render_template("testgraphtree.html", response = json.dumps(jsonresponse)) # render the tree
  except:
    return render_template("graph-noresult.html", response = username)  # in case there is no result

def get_graph(username):
  fetch_data.get_user_following(username) # calling the function in fetch_data.py to get data from API
  return 

if __name__ == "__main__":
  app.run(host='0.0.0.0', debug=True)

