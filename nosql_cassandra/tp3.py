import os
from abc import ABC

from cassandra.cluster import Cluster
from cassandra.cluster import DCAwareRoundRobinPolicy
from cassandra.cluster import Session

from no_sql.main import ImporterCSV


class CassandraImporter(ImporterCSV, ABC):
  cluster: Cluster = None
  session: Session = None

  def connect(self):
    contact_points = os.environ['CASSANDRA_HOSTS'].split(',')
    local_dc = os.environ['CASSANDRA_DATACENTER']
    self.cluster = Cluster(
      contact_points=contact_points,
      load_balancing_policy=DCAwareRoundRobinPolicy(local_dc))
    self.session = self.cluster.connect()

  def close(self):
    self.cluster.shutdown()
