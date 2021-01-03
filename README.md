# EnrichDB: A Database System for Progressive Data Enrichmentand Query Evaluation 

Full version of the paper will be available by 6th January, 2021.

### Installlation Using Docker

```
git clone <enrichdb git repo link>
cd ..
sudo docker build -t tagdb tagdb/
```

Connect to  postgresql running tagdb inside docker 
```
sudo docker exec -it tag_con bash
su - postgres 
/usr/local/pgsql/bin/psql test
```
Run UI to execute and visualize queries

```
cd ui
python server.py  # requires python2

UI is accessible at localhost:8000
```
** EnrichDB code is based on Apache MadLib

### Example Query Execution Using EnrichDB UI
```
SELECT tweets.id as tweet_id, tweets.tweet_object as tweet_object, tweets.location as tweet_location, 
tweets.timestamp as tweet_time, tweets.sentiment as tweet_sentiment @FROM tweets 
@WHERE  tweets.sentiment = 0 AND tweets.id < 700
```
![](tagdb-ui.gif)

