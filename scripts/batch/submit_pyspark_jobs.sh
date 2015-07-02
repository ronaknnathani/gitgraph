#!/usr/bin/env bash
# this script is used to submit pyspark jobs
# e.g.: ./submit_pyspark_jobs.sh script.py

/usr/local/spark/bin/spark-submit --master spark://$master_ip:7077 $1
