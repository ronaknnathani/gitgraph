# [GitHub Graph] (http://githubgraph.com/)
 
*Work in Progress*

## Index
1. [Introduction] (README.md#1-introduction)
2. [AWS Clusters] (README.md#2-aws-clusters)
3. [Data Collection and Ingestion] (README.md#3 -data-collection-and-ingestion)
4. [Batch Processing] (README.md#4 - batch-processing)
5. [Serving Layer] (README.md#5 - serving-layer)
6. [Front End] (README.md#6 - front-end)

## 1. Introduction
GitHub Graph is a tool to enable developers to stay updated with the trends on GitHub that they would be interested in. It uses [GitHub Archive] (https://www.githubarchive.org/) and [GitHub API] (https://developer.github.com/v3/users/) as data sources to serve the application. Esentially, it is a big data pipeline focused on answering- "For the users I follow, what are the repositories that those users follow and contribute to".

![Example graph query] (flask/static/img/graph.png)

GitHub Graph also shows top 10 weekly trending repositories based on the number of stars.

![Weekly Trending repos] (flask/static/img/trending.png)

### Data Sources
* [GitHub Archive] (https://www.githubarchive.org/): 
[Ilya Grigorik] (https://www.igvita.com/) started the GitHub Archive project to record the public GitHub timeline, archive it, and make it easily accessible for further analysis. It has a very simple API to collect data on the granularity of an hour. GitHub Graph pulled 850+ GB of data from this source ranging from December 2011 till today. These are clean nested json responses with one event on each line. Events for example are 'WatchEvent', 'ForkEvent', 'CommitCommentEvent', etc.

![Weekly Trending repos] (flask/static/img/publictimeline.png)

An example to collect acttivity for all of January 2015	from [GitHub Archive] (https://www.githubarchive.org/) is:

    $ wget http://data.githubarchive.org/2015-01-{01..30}-{0..23}.json.gz

* [GitHub API] (https://developer.github.com/v3/users/): 
GitHub Graph has 12M+ GitHub usernames with their IDs from GitHub API's https://api.github.com/users endpoint. Using these  usernames it also collects data regarding who these users are following using https://api.github.com/users/username/following endpoint. There are over 4M such records in the database at present. Moreover, if the queried username is not in the database, records for that username are collected from the API in real time, stored in the database and results are generated.

GitHub's API has a rate limit of 5000 calls/hour and around 25 GitHub API access tokens were used to collect data for GitHub Graph. Thanks to my fellow fellows at Insight and friends in India.

## 2. AWS Clusters 
GitHub Graph is powered by three clusters on AWS-
* 3 m3.large producers collecting data from GitHub's API and sending these messages to Kafka

![Producers] (/flask/static/img/producers.png)

* 6 m3.xlarge for Spark Cluster
* 5 m3.large for HDFS, Kafka, Cassandra, Zookeeper

![Batch] (/flask/static/img/clusters.png)

## 3. Data Pipeline

![Pipeline] (/flask/static/img/pipeline.png)

 * ### Data Collection and Ingestion 
  * The data from GitHub Archive is stored on HDFS with 4 data nodes and 1 name node. 
  
  * 3 producers are collecting data from GitHub's API and shooting messages to Kafka. These messages are consumed using [camus] (https://github.com/linkedin/camus). [Camus] (https://github.com/linkedin/camus) is a tool built by [Linkedin] (https://www.linkedin.com/) which is essentially a distributed consumer running a map reduce job underneath to consume messages from Kafka and save them to HDFS.
  
  Camus is really great for the Kafka->HDFS pipeline as it keeps a track of the last offset consumed for a topic and also allows to whitelist and blacklist topics so that one can consume only a subset of topics. Moreover, Camus also compresses the data before saving it to HDFS which saves space by an order of magnitude. Camus is very easy to set up and is worth the time spent, however, one important thing to note while setting up Camus is that the camus jar and log4j.xml must be in HADOOP's path to run camus. 

 * ### Batch Processing
  Spark SQL is used here for all the batch processing. For the data from GitHub Archive, events like 'WatchEvent', 'ForkEvent', 'CommitCommentEvent' ae filtered as they are representative of the fact that a user has either contributed to a repository or is following one. 

  The data from GitHub archive has inconsistent schema. It is filtered in the batch process to a two column schema with a user and a list of all the repositories that he/she has contributed to or is following. Below is an example of schema from 2015 and 2012 and the resulting two column schema.
  
![Pipeline] (/flask/static/img/inconsistent\ schema.png)

  The two column schema form the batch process is used to create a table in Cassandra
![Pipeline] (/flask/static/img/userrepo.png)
  
  GitHub's API returns many fields in its json response for usernames and IDs which are not really important for the application. So, 12GB of raw data for 12M+ users is reduced to 450MB by extrcting login names and IDs in the batch process from the raw responses and is further compressed while consumed by Camus to 122MB.
  
![Pipeline] (/flask/static/img/users.png)

  These usernames are used to pull records of who these users are following to create the Cassandra table below.
  
![Pipeline] (/flask/static/img/userfollowing.png)

  Weekly trends are learned from 'WatchEvents' in GitHub Archive and a table with repo names and corresponding watch counts is created.

 * ### Serving Layer
  I use Cassandra to save my batch results. I have three main tables in Cassanrdra
  * Userrepo- Key is the username and value is the list of repos that the user follows and has contributed to.
  * Userfollow - Key is the username and value is the list of usernames of the people who user follows.
  * Weeklytrends - Key is the reponame and value are the watch counts in the past week.

## 4. Front end
Flask is use for the web app and D3 to visualize results.

## 5. Presentation
Presentation for GitHub Graph can be found here - http://www.slideshare.net/ronaknnatnani/githubgraph

I encourage you to play with [GitHub Graph](http://githubgraph.com/) and see trends you would be interested in.



