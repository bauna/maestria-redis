import logging
import os

import pytest
from pymongo import MongoClient
from testcontainers.mongodb import MongoDbContainer


@pytest.fixture(scope='session', autouse=True)
def mongo_container(request):
  mongo_container = MongoDbContainer()
  request.addfinalizer(mongo_container.stop)
  mongo_container.start()
  os.environ['MONGO_URL'] = mongo_container.get_connection_url()
  return mongo_container

@pytest.fixture(scope='session', autouse=True)
def mongo_client(request, mongo_container):
  mongo_client = MongoClient(mongo_container.get_connection_url())
  request.addfinalizer(mongo_client.close)
  return mongo_client

@pytest.fixture(scope='function', autouse=True)
def setup_data(mongo_client):
  system_dbs = {'admin', 'local', 'config'}
  for db_name in mongo_client.list_database_names() :
    if db_name not in system_dbs:
      logging.info('dropping database: %s', db_name)
      mongo_client.drop_database(db_name)
