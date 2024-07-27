from nosql_mongo.tp4_ej01 import MongoImporterEJ01


class TestMongoEJ01:

  def test_get(self, base_path, mongo_client):
    db = mongo_client.tp04_ej01
    importer = MongoImporterEJ01()
    # importer.process_csv(f'{curr_path}/full_export.csv')
    importer.process_csv(f'{base_path}/tests/full_export_version_corta.csv')

    # test_data = {10: 'Yohan BLAKE', 20: 'Tyquendo TRACEY', 30: 'Carina HORN'}
    test_data = {'1': 'Christian COLEMAN', '2': 'Noah LYLES', '3': 'Divine ODUDURU'}
    for key, name in test_data.items():
      query = {'_id': key}
      deportista = list(db.deportistas.find(query))[0]
      assert name == deportista['nombre_deportista']
