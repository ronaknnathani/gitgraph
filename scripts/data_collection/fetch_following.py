#!/usr/bin/env python

# importing requests & urllib2 library and authentication modules
import json
from requests.auth import HTTPBasicAuth
from urllib2 import urlopen, Request
import os
import requests
import time
from kafka import SimpleProducer, KafkaClient
import gzip

# setup Kafka client and producer
start = time.time()
kafka = KafkaClient('ec2-52-8-111-39.us-west-1.compute.amazonaws.com:9092')
producer = SimpleProducer(kafka)

# getting user credentials for authentication
github_user_vimal = os.environ['github_vimal']
github_pass_vimal = os.environ['github_vimal_pass']

github_user_ujjval = os.environ['github_ujjval']
github_pass_ujjval = os.environ['github_ujjval_pass']

# url for API calls
following_url = "https://api.github.com/users/"

# flag to get 100 results per page
per_page="&per_page=100"

# making API calls to fetch user->following records
# url_tail stores the page number being fetched for each user and... 
# if the response is more than a page, it is incremented by 1 to...
# fetch next page and so on. Maximum 10 pages for a user are fetched
# user is the username for whom records are being collected
# page is used in the recursive call to update the page number being collected
# alternate between two credentials for every 4999 calls

i=0
def get_following(url_tail, user, following, page):
  global i
  global github_user_vimal
  global github_user_ujjval
  global github_pass_vimal
  global github_pass_ujjval
  if i >= 4999:
    json_response = []
    github_user = github_user_vimal
    github_pass = github_pass_vimal
    try:
      response = requests.get(following_url + user + "/following" + url_tail + per_page, auth=HTTPBasicAuth(github_user, github_pass), timeout=5) # make the API call with basic authentication (password used)
      json_response = json.loads(response.text) # convert text to Python dict
    except requests.exceptions.Timeout as e: # take care of exceptions
      time.sleep(30)
      return following
    except requests.exceptions.ConnectionError as e:
      time.sleep(30)
      return following
    except requests.exceptions.HTTPError as e:
      time.sleep(30)
      return following
    except Exception, e:
      print e
      return following
    i+=1
    if i==9998:
      i = 0
  else:
    json_response = []
    github_user = github_user_ujjval
    github_pass = github_pass_ujjval
    try:
      request = Request(following_url + user + "/following" + url_tail + per_page) # make the API call 
      request.add_header('Authorization', 'token %s' % github_pass)# authorize the call (personall access token used)
      response = urlopen(request, timeout=5)
      json_response = json.loads(response.read()) # convert json to Python dict
    except requests.exceptions.Timeout as e: # take care of exceptions
      time.sleep(30)
      return following
    except requests.exceptions.ConnectionError as e:
      time.sleep(30)
      return following
    except requests.exceptions.HTTPError as e:
      time.sleep(30)
      return following
    except Exception, e:
      print e
      return following
    i+=1
  if json_response == [] or page==10: # collect max 10 pages per user
    return following # return the list if it is empty or 10 pages collected
  else:
    following += json_response # concatenate results to previous page's result
    page +=1 # increment page after each call
    get_following("?page="+str(page), user, following, page) # update the url and start recursion
    return following # return the compiled result

# read the usernames from a file and call API function to collect corresponding details
count = 0
with open("following.json","w") as op_file: 
  with gzip.open("../data/github-usernames.gz", "r") as ip_file: 
    for line in ip_file:
      users = json.loads(line) 
      user = users["login"] 
      user_id = users["id"] 
      print "\ncount: ", count, "id: : ", user_id
      if user_id < 2000000 or user_id > 4000000:
        continue
      following = []
      following_dict = {}
      following = get_following("?page=1", user, [], 1)
      following_dict["login"] = user
      following_dict["id"] = user_id
      following_dict["following"] = following
      print following_dict
      count += 1
      op_file.write(json.dumps(following_dict))
      producer.send_messages("scrap-following-3", json.dumps(following_dict))
      print "total people followed: ", len(following)

stop = time.time()
print stop-start


