import os
from abc import ABC

from cassandra.cluster import Cluster
from cassandra.cluster import DCAwareRoundRobinPolicy
from cassandra.cluster import Session

from no_sql.main import ImporterCSV
from nosql_cassandra.tp3 import CassandraImporter


class CassandraImporterEJ01(CassandraImporter):

  def initialize(self):
    cassandra_session = self.session
    cassandra_session.execute(
      f'''CREATE KEYSPACE IF NOT EXISTS tp3_ej1 
        WITH replication = {
          'class': 'SimpleStrategy', 
          'replication_factor': 1
        };''')
    cassandra_session.execute('USE tp3_ej1')
    cassandra_session.execute('DROP TABLE IF EXISTS deportistas;')
    cassandra_session.execute(
      'CREATE TABLE IF NOT EXISTS deportistas (id INT, nombre TEXT, PRIMARY KEY (id));')

  def process_row(self, row):
    self.session.execute(
      f'''INSERT INTO deportistas (id, nombre) VALUES 
          ({row['id_deportista']}, {row['nombre_deportista']});''')
