#!/usr/bin/env python

# importing requests library and authentication modules 
from requests.auth import HTTPBasicAuth
import requests
import json
import time
import os

# getting user credentials for authentication
github_user_insight = os.environ['github_insight1']
github_pass_insight = os.environ['github_insight1_pass']

github_user_personal = os.environ['github_user']
github_pass_personal = os.environ['github_pass']

start = time.time()
index = 1
per_page = "&per_page=100"

# making API calls to get user details
# index is the last user id fetched
# githb_user is the username for the credential
# github_pass is the password for the credential
# j is the number of calls made

def call_github(index, github_user, github_pass, j):
  url = "https://api.github.com/users?since="+str(index)+per_page # url formatting to start from a specific index and get 100 results per page
  response = requests.get(url, auth=HTTPBasicAuth(github_user, github_pass)) # making API call with basic authentication
  jsondata = json.loads(response.text) # converting received text into Python dict
  next_index = jsondata[-1]["id"] # getting the last user id received
  op_file = "../data/users_data/users" + str(index) # writing data to files for every call
  with open(op_file, 'w') as outputfile:
    json.dump(jsondata, outputfile)
  print next_index, j # checking the last index and number of calls made
  return next_index

# alternate between two credentials for every 4999 calls
i = 0
while True:
  if i<4999:
    index = call_github(index, github_user_insight, github_pass_insight, i) 
    i+=1
  else:
    index = call_github(index, github_user_personal, github_pass_personal, i)
    i+=1
    if i==9998:
      i=0

  
stop = time.time()
print stop-start
