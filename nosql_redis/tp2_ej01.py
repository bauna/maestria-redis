from nosql_redis.tp2 import RedisImporter

class RedisImporterEJ01(RedisImporter):
  def process_row(self, row):
    self.db.set(row['id_deportista'], row['nombre_deportista'], nx = True)
