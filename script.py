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
import datetime
import json
import logging
import os, os.path
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import sys


#
# Config
#
user = sys.argv[1]
log_file = 'twitter2datashare.log'
log_level = logging.DEBUG
input_file = user + '.json'
output_file = 'requests'
index = 'local-datashare'
author = 'https://twitter.com/' + user + '/'
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

def delete_all_files():
    for root, dirs, files in os.walk(tweets_folder):
        for file in files:
            os.remove(os.path.join(root, file))

def delete_documents_from_elasticsearch(es):
    es.delete_by_query(index=index,
                       body='{"query": {"match": {"dirname": "' + path_project + path_twitter + '"}}}')

def main():
    es = Elasticsearch()
    delete_documents_from_elasticsearch(es)
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
                        "tika_metadata_url": "https://twitter.com/" + user + "/status/" + str(tweet['id'])
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
    bulk(es, actions)

#
# Main
#
if __name__ == "__main__":
    logging.basicConfig(filename=log_file, filemode='w+', format='%(asctime)s  |  %(levelname)s  |  %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=log_level)
    logging.info('Start')
    main()
