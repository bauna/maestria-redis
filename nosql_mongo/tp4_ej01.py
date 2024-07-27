from nosql_mongo.tp4 import MongoImporter


class MongoImporterEJ01(MongoImporter):
  db = None

  def initialize(self):
    self.db = self.client.tp04_ej01

  def process_row(self, row):
    query = {'_id': row['id_deportista']}

    document = {'_id': row['id_deportista'],
                'nombre_deportista': row['nombre_deportista'],
                'fecha_nacimiento': row['fecha_nacimiento'],
                'id_pais_deportista': row['id_pais_deportista'],
                'nombre_pais_deportista': row['nombre_pais_deportista']}
    newvalues = {"$set": document}
    self.db.deportistas.update_one(query, newvalues, upsert=True)
