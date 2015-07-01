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
GitHub hosts maximum number of open source repositories and has more than 12.5M users. As a Data Engineer, I use open source technologies and am interested in being updated with what is trending. And it would be great if this could be personalized. To scratch this itch, I built GitHub Graph.

GitHub Graph is a big data pipeline focused on answering- "For the users I follow, what are the repositories that those users follow and contribute to".



### Data Sources
* [GitHub Archive] (https://www.githubarchive.org/)
[Ilya Grigorik] (https://www.igvita.com/) started the GitHub Archive project to record the public GitHub timeline, archive it, and make it easily accessible for further analysis. It has a very nice simple API to collect data on an hourly basis. I collected 850+ GB of data from this source. The data ranges from December 2011 to June 2015.

For e.g., Activity for all of January 2015	can be collected using:

    $ wget http://data.githubarchive.org/2015-01-{01..30}-{0..23}.json.gz

* [GitHub API] (https://developer.github.com/v3/users/)
I collected 12M+ usernames witht their IDs from GitHub API's (https://api.github.com/users) endpoint. Using these  usernames I collected data regarding who these users are following using (https://api.github.com/users/<username>/following) endpoint. I have 3M+ of these records. 

GitHub's API rate limits me at 5000 calls/hour and I have around 25 GitHub API access token collecting data. Thanks to my fellow fellows.

## 2. AWS Clusters 
I used three clusters on AWS-
* 6 m3.xlarge for Spark Cluster
* 5 m3.large for HDFS, Kafka, Cassandra, Zookeeper

## 3. Data Collection and Ingestion 
* The data from GitHub Archive is stored on HDFS with 4 data nodes and 1 name node. 

* I have 3 producers collecting data from GitHub's API and shooting messages to Kafka. I consume these messages using [camus] (https://github.com/linkedin/camus). [Camus] (https://github.com/linkedin/camus) is a tool built by [Linkedin] (https://www.linkedin.com/) which is essentially a distributed consumer running a map reduce job underneath to consume messages from Kafka and safe them to HDFS.

Camus is really great for the Kafka->HDFS pipeline as it keeps a track of the last offset consumed for a topic and also allows to whitelist and blacklist topics so that one can consume only a subset of topics. Moreover, Camus also compresses the data before saving it to HDFS which saves space by an order of magnitude. Camus is very easy to set up and is worth the time spent, however, one important thing to note while setting up Camus is that the camus jar and log4j.xml must be in HADOOP's path to run camus. 

## 4. Batch Processing
I used Spark SQL for my batch processing. For the data from GitHub Archive, I filter events like 'WatchEvent', 'ForkEvent', 'CommitCommentEvent' as they are representative of the fact that a user has either contributed to a repository or is following one. With these filtered events, I run a Spark job to create a two column schema with a user and a list of all the repositories that he/she has contributed to or is following. 

The schema from GitHub Archive is inconsistent. So, I filter data from every year to just the columns I want and then run the Spark job mentioned above. The output of this is saved in Cassandra table with "username" as the primary key and list of repositories for that username as a column.

I also use Spark SQL to filter out data from the users->following records as GitHub's API returns many json fields that are not important to the application. So, I just bring it down to a json record containing "username" and usernames of all the people followed by the user "username".

## 5. Serving Layer
I use Cassandra to save my batch results. I have two main tables in Cassanrdra
* Userrepo- Key is the username and value is the list of repos that the user follows and has contributed to.
* Userfollow - Key is the username and value is the list of usernames of the people who user follows.
* Weeklytrends - Key is the reponame and value are the watch counts in the past week.

## 6. Front end
I use Flask for the web app and D3 to visualize results.

## 7. Presentation
My presentation can be found here - http://www.slideshare.net/ronaknnatnani/githubgraph



