import os

from nosql_redis.tp1_ej01 import RedisImporterTP01


class TestTP01:

  def test_get(self, redis_client):
    importer = RedisImporterTP01()
    base_path = os.getcwd()
    # importer.process_csv(f'{curr_path}/full_export.csv')
    importer.process_csv(f'{base_path}/tests/full_export_version_corta.csv')

    # test_data = {10: 'Yohan BLAKE', 20: 'Tyquendo TRACEY', 30: 'Carina HORN'}
    test_data = {1: 'Christian COLEMAN', 2: 'Noah LYLES', 3: 'Divine ODUDURU'}
    for key, name in test_data.items():
      assert name == redis_client.get(key).decode('utf-8')
