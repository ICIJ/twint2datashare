#!/usr/bin/python
# -*- coding: utf-8 -*-
# Execution example : python script.py
# 1. Extract all tweets from user with twint
# twint -u USERNAME -o USERNAME.json --json
# Bulk insertion into Elasticsearch
# https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
# curl -s -H "Content-Type: application/x-ndjson" -XPOST localhost:9200/_bulk --data-binary "@requests"
# Elasticsearch API for Python : https://elasticsearch-py.readthedocs.io/en/master/index.html


#
# Libs
#
import click
import datetime
import elasticsearch
from elasticsearch import helpers
import json
import logging
import os
import sys


#
# Config
#
log_file = 'twitter2datashare.log'
log_level = logging.DEBUG
output_file = 'requests'
path_project = '/home/dev/data'
path_twitter = '/social_media/twitter'
tweets_folder = 'tweets'


#
# Functions
#
def get_current_time():
    t = datetime.datetime.now()
    s = t.strftime('%Y-%m-%dT%H:%M:%S.%f')
    return s[:-3] + 'Z'

@click.command()
@click.option("--username", prompt="Twitter username", help="Twitter username.")
@click.option("--index", default='local-datashare', help="Elasticsearch index.")
@click.option("--host", default='localhost', help="Elasticsearch host.")
@click.option("--port", default='9200', help="Elasticsearch port.")
def main(username, index, host, port):
    input_file = username + '.json'
    author = 'https://twitter.com/' + username + '/'
    es = elasticsearch.Elasticsearch(hosts=[{'host': host, 'port': port}])
    delete_documents_from_elasticsearch(es, index)
    delete_all_files()
    actions = []
    with open(input_file) as json_file:
        tweets = json.load(json_file)
        for tweet in tweets:
            tweet_file = tweet['tweet'][:40].replace('/', '').replace('"', '') + '_' + str(tweet['id']) + '.json'
            object = {
                "_op_type": "create",
                "_index": index,
                "_type": "doc",
                "_id": str(tweet['id']),
                "_source": {
                    "content": tweet['tweet'].replace('"', '\\"').replace('\r', ''),
                    "metadata": {
                        "tika_metadata_creation_date": tweet['date'] + 'T' + tweet['time'] + 'Z',
                        "tika_metadata_author": author,
                        "tika_metadata_url": "https://twitter.com/" + username + "/status/" + str(tweet['id'])
                    },
                    "type": "Document",
                    "contentType": "application/json",
                    "language": "ENGLISH",
                    "extractionLevel": 0,
                    "dirname": path_project + path_twitter,
                    "path": path_project + path_twitter + '/' + tweet_file,
                    "extractionDate": get_current_time(),
                    "status": "INDEXED",
                    "nerTags": [],
                    "join": {
                        "name": "Document"
                    }
                }
            }
            actions.append(object)
            file = open(os.path.join(tweets_folder, tweet_file), 'w')
            file.write(json.dumps(tweet, indent=4))
            file.close()
    helpers.bulk(es, actions)

def delete_all_files():
    for root, dirs, files in os.walk(tweets_folder):
        for file in files:
            os.remove(os.path.join(root, file))

def delete_documents_from_elasticsearch(es, index):
    es.delete_by_query(index=index,
                       body='{"query": {"match": {"dirname": "' + path_project + path_twitter + '"}}}')

#
# Main
#
if __name__ == "__main__":
    logging.basicConfig(filename=log_file, filemode='w+', format='%(asctime)s  |  %(levelname)s  |  %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=log_level)
    logging.info('Start')
    main()
