import logging
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
    cassandra_hosts = os.environ['CASSANDRA_HOSTS']
    logging.info('Cassandra hosts: %s', cassandra_hosts)
    contact_points = [(ip, int(port)) for ip, port in
                      [address.split(':') for address in cassandra_hosts.split(',')]]
    local_dc = os.environ['CASSANDRA_DATACENTER']
    self.cluster = Cluster(contact_points,
      load_balancing_policy=DCAwareRoundRobinPolicy(local_dc))
    self.session = self.cluster.connect()

  def close(self):
    self.cluster.shutdown()
