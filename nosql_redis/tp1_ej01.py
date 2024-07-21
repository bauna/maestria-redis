from nosql_redis.tp1 import RedisImporter

class RedisImporterTP01(RedisImporter):
  def process_row(self, row):
    self.db.set(row['id_deportista'], row['nombre_deportista'], nx = True)
