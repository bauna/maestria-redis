import os

from nosql_redis.tp1_ej01 import RedisImporterTP01


class TestTP01:

  def test_get(self, redis_client):
    importer = RedisImporterTP01()
    curr_path = os.getcwd()
    importer.process_csv(f'{curr_path}/full_export.csv')

    test_data = {10: "Yohan BLAKE", 20: "Tyquendo TRACEY", 30: "Carina HORN"}
    for key, name in test_data.items():
      assert name == redis_client.get(key).decode('utf-8')
