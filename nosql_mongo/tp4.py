import os
from abc import ABC

from pymongo import MongoClient
from no_sql.main import ImporterCSV


class MongoImporter(ImporterCSV, ABC):
  client: MongoClient = None

  def connect(self):
    self.client = MongoClient(os.environ['MONGO_URL'])

  def close(self):
    self.client.close()
