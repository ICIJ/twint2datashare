#!/usr/bin/python
# -*- coding: utf-8 -*-


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
import shutil


#
# Config
#
log_file = 'twint2datashare.log'
log_level = logging.DEBUG
path_twitter = 'Social Media/Twitter'
tweets_folder = 'tmp'


#
# Functions
#
def get_current_time():
    t = datetime.datetime.now()
    s = t.strftime('%Y-%m-%dT%H:%M:%S.%f')
    return s[:-3] + 'Z'


def delete_all_files_from_folder(folder):
    for root, dirs, files in os.walk(folder):
        for file in files:
            os.remove(os.path.join(root, file))


def copy_all_files(filespath):
    os.makedirs(os.path.dirname(os.path.join(filespath, path_twitter, 'random')), exist_ok=True)
    for root, dirs, files in os.walk(tweets_folder):
        for file in files:
            source = os.path.join(tweets_folder, file)
            destination = os.path.join(filespath, path_twitter, file)
            shutil.copyfile(source, destination)


def delete_documents_from_elasticsearch(es, index, filespath):
    es.delete_by_query(index=index,
                       body='{"query": {"match": {"dirname": "' + os.path.join(filespath, path_twitter) + '"}}}')

def delete_folder(folder):
    shutil.rmtree(folder)


@click.command()
@click.option('--username', prompt='Twitter username', help='Twitter username. Mandatory.')
@click.option('--index', default='local-datashare', help='Elasticsearch index. Default: local-datashare.')
@click.option('--host', default='localhost', help='Elasticsearch host. Default: localhost.')
@click.option('--port', default='9200', help='Elasticsearch port. Default: 9200.')
@click.option('--filesPath', default='/home/dev/data', help='Path where files are stored. Default : "/home/dev/data".')
def main(username, index, host, port, filespath):
    input_file = username + '.json'
    author = 'https://twitter.com/' + username + '/'
    es = elasticsearch.Elasticsearch(hosts=[{'host': host, 'port': port}])
    delete_documents_from_elasticsearch(es, index, filespath)
    delete_all_files_from_folder(tweets_folder)
    delete_all_files_from_folder(os.path.join(filespath, path_twitter))
    actions = []
    os.makedirs(os.path.dirname(os.path.join(tweets_folder, 'random')), exist_ok=True)
    with open(input_file) as file:
        for line in file:
            tweet = json.loads(line)
            tweet_file = tweet['tweet'][:40].replace('/', '').replace('"', '').replace('|', '') + '_' + str(tweet['id']) + '.json'
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
                    "dirname": os.path.join(filespath, path_twitter),
                    "path": os.path.join(filespath, path_twitter, tweet_file),
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
    copy_all_files(filespath)
    delete_folder(tweets_folder)


#
# Main
#
if __name__ == "__main__":
    logging.basicConfig(filename=log_file, filemode='w+', format='%(asctime)s  |  %(levelname)s  |  %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=log_level)
    logging.info('Start')
    main()
