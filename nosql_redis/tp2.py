import os
from abc import ABC

from redis import Redis
from no_sql.main import ImporterCSV

class RedisImporter(ImporterCSV, ABC):
  db: Redis = None

  def connect(self):
    self.db = Redis(
      host=os.environ['REDIS_HOST'],
      port=int(os.environ['REDIS_PORT']),
      decode_responses=True)

  def close(self):
    self.db.close()


