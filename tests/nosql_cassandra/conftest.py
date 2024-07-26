import logging
import os
import pytest

from testcontainers.cassandra import CassandraContainer
from cassandra.cluster import Cluster
from cassandra.cluster import DCAwareRoundRobinPolicy


@pytest.fixture(scope='session', autouse=True)
def cassandra_container(request):
  cassandra_container = CassandraContainer()
  cassandra_container.start()
  request.addfinalizer(cassandra_container.stop)
  contact_points = cassandra_container.get_contact_points()
  os.environ['CASSANDRA_HOSTS'] = ','.join(
    [f'{ip}:{port}' for ip, port in contact_points])
  os.environ['CASSANDRA_DATACENTER'] = cassandra_container.get_local_datacenter()
  return cassandra_container


@pytest.fixture(scope='session', autouse=True)
def cassandra_cluster(request, cassandra_container):
  contact_points = cassandra_container.get_contact_points()
  local_dc = cassandra_container.get_local_datacenter()
  cluster = Cluster(contact_points, load_balancing_policy=DCAwareRoundRobinPolicy(local_dc))
  request.addfinalizer(cluster.shutdown)
  return cluster

@pytest.fixture(scope='session', autouse=True)
def cassandra_session(cassandra_container, cassandra_cluster):
  session = cassandra_cluster.connect()
  result = session.execute('SELECT release_version FROM system.local;')
  version = result.one().release_version
  logging.info('Cassandra version: %s', version)
  return session


@pytest.fixture(scope='function', autouse=True)
def setup_data(cassandra_session):
  rows = cassandra_session.execute('desc keyspaces')
  user_keyspaces = [ks.name for ks in rows if not ks.name.startswith('system')]
  for ks in user_keyspaces:
    logging.info('dropping keyspace: %s', ks)
    cassandra_session.execute(f'drop keyspace {ks};')
