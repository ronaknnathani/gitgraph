#!/usr/bin/env python

import json
import os
from kafka import SimpleProducer, KafkaClient

# Set up Kafka
kafka = KafkaClient("ec2-52-8-111-39.us-west-1.compute.amazonaws.com:9092")
producer = SimpleProducer(kafka)

# reading user records from files and sending login names and IDs as messages to Kafka
count = 0
with open("final_usernames_file","w") as op_file:
  files = os.listdir("../../data/users_data")
  for i, filename in enumerate(files):
    if filename.startswith("users"):
      with open("../../data/users_data/"+filename, "r") as ip_file:
        try:
   	  print "\nfrom file: ", filename
          json_records = json.load(ip_file)
          for user in json_records:
            user_dict = {}
            user_dict["login"] = user["login"]
            user_dict['id'] = user["id"]
	    producer.send_messages("github-usernames-good-1", json.dumps(user_dict))
	    op_file.write(json.dumps(user_dict))
      	    print "count: ", count, "id: ", user_dict["id"]
	    count += 1
        except:
	  print "Some error in file: ", filename
	  error_file.write("Some error in file: %s " % filename)

error_file.close()
