# GitHub Graph
 
*Work in Progress*

## Index
1. [Introduction] (README.md#1-introduction)
2. Data Collection and Ingestion
3. Batch Processing 
4. Serving Layer
5. Front End

## 1. Introduction
GitHub hosts maximum number of open source repositories and has more than 12.5M users. As a Data Engineer, I use open source technologies and would like to be updated with what is trending. And it would be great if this could be personalized. To scratch this itch, I built GitHub Graph.

GitHub Graph is a big data pipeline focused on answering- "For the users I follow, what are the repositories that those users follow and contribute to".

### Data Sources
* [GitHub Archive] (https://www.githubarchive.org/)
[Ilya Grigorik] (https://www.igvita.com/) started the GitHub Archive project to record the public GitHub timeline, archive it, and make it easily accessible for further analysis. It has a very nice simple API to collect data on an hourly basis. I collected 850+ GB of data from this source. The data ranges from December 2011 to June 2015.

For e.g., Activity for all of January 2015	wget http://data.githubarchive.org/2015-01-{01..30}-{0..23}.json.gz

* [GitHub API] (https://developer.github.com/v3/users/)
I collected 12M+ usernames witht their IDs from GitHub API's (https://api.github.com/users) endpoint. Using these  usernames I collected data regarding who these users are following using (https://api.github.com/users/<username>/following) endpoint. I have 3M+ of these records. 

GitHub's API rate limits me at 5000 calls/hour and I have around 25 GitHub API access token collecting data. Thanks to my fellow fellows.

## 2. Data Collection and Ingestion (README.md#2 - data collection and ingestion)
* The data from GitHub Archive is stored on HDFS with 4 data nodes and 1 name node. 
* I have 3 producers collecting data from GitHub's API and shooting messages to Kafka. I consume these messages using [camus] (https://github.com/linkedin/camus). [Camus] (https://github.com/linkedin/camus) is a tool built by [Linkedin] (https://www.linkedin.com/) which is essentially a distributed consumer running a map reduce job underneath to consume messages from Kafka and sae it to HDFS.



