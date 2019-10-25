
# What's before all of that ?
Just use [twint](https://github.com/twintproject/twint) to extract tweets from USERNAME's account

`twint -u USERNAME -o USERNAME.json --json`

# How to use it
## See help
`python script.py --help`

## Call script
`python script --username TWITTER_USERNAME --index ES_INDEX --host ES_HOST --port ES_PORT --filesPath FILE_PATH`

* username (mandatory) : username from where you extracted the tweets with twint.
* index : Elasticsearch index name, default is local-datashare.
* host : Elasticsearch host, default is 9200.
* port : Elasticsearch port, default is 9200.
* filesPath: Path where files are stored, default is "/home/dev/data".