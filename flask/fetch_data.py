#!/usr/bin/env python

import json
from requests.auth import HTTPBasicAuth
from urllib2 import urlopen, Request
import os
import requests
import time
import gzip
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table

class userfollow(Model):
  username = columns.Text(primary_key=True)
  following = columns.List(columns.Text)
  def __repr__(self):
    return '%s %d' % (self.username, self.following)

connection.setup(['52.8.127.252','52.8.41.216'], "watch_events")
sync_table(userfollow)

start = time.time()

github_user_alvin = "alvin"
github_pass_alvin = os.environ['alvin_pass']

github_user_vimal1 = "vimal1"
github_pass_vimal1 = os.environ['vimal1_pass']

following_url = "https://api.github.com/users/"

per_page="&per_page=100"

f = open("error_fetch_following_alvin_vimal1","w")
i=0
def get_following(url_tail, user, following, page):
  global i
  global github_user_alvin
  global github_user_vimal1
  global github_pass_alvin
  global github_pass_vimal1
  if i < 4999:
    json_response = []
    github_user = github_user_alvin
    github_pass = github_pass_alvin
    try:
      request = Request(following_url + user + "/following" + url_tail + per_page)
      request.add_header('Authorization', 'token %s' % github_pass)
      response = urlopen(request, timeout=5)
      json_response = json.loads(response.read())
    except requests.exceptions.Timeout as e:
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
  else:
    json_response = []
    github_user = github_user_vimal1
    github_pass = github_pass_vimal1
    try:
      request = Request(following_url + user + "/following" + url_tail + per_page)
      request.add_header('Authorization', 'token %s' % github_pass)
      response = urlopen(request, timeout=5)
      json_response = json.loads(response.read())
    except requests.exceptions.Timeout as e:
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
  if json_response == [] or page==10:
    return following
  else:
    following += json_response
    page +=1
    get_following("?page="+str(page), user, following, page)
    return following

def follows(x):
    try:
        following_list = []
        if x == []:
            return following_list
        else:
            for user in x:
                following_list.append(user['login'])
            return following_list
    except Exception as e:
        print x, e
    return following_list

def get_user_following(username):
    user = username
    following = []
    following_dict = {}
    following = get_following("?page=1", user, [], 1)
    following_dict["login"] = user
    following_dict["following"] = following
    # print following
    # print type(following[0])
    # print "total people followed: ", len(following)
    # print "\n", follows(following)
    userfollow.create(username=user, following = follows(following))
    return

if __name__ == "__main__":
  main()
