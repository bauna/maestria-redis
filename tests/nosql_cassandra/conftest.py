import logging
import os
import pytest

from testcontainers.cassandra import CassandraContainer
from cassandra.cluster import Cluster
from cassandra.cluster import DCAwareRoundRobinPolicy


@pytest.fixture(scope='session', autouse=True)
def cassandra_container(request):
  with CassandraContainer('cassandra:4.1.4') as cassandra_container:
    cassandra_container.start()
    os.environ['CASSANDRA_HOSTS'] = ','.join(
      [f'{ip}:{port}' for ip, port in cassandra_container.get_contact_points()])
    os.environ['CASSANDRA_DATACENTER'] = cassandra_container.get_local_datacenter()
    yield cassandra_container
    cassandra_container.stop()


@pytest.fixture(scope='session', autouse=True)
def cassandra_cluster(cassandra_container):
  contact_points = cassandra_container.get_contact_points()
  local_dc = cassandra_container.get_local_datacenter()
  with (Cluster(contact_points, load_balancing_policy=DCAwareRoundRobinPolicy(local_dc))
        as cluster):
    yield cluster
    cluster.shutdown()


@pytest.fixture(scope='session', autouse=True)
def cassandra_session(cassandra_container, cassandra_cluster):
  session = cassandra_cluster.connect()
  result = session.execute('SELECT release_version FROM system.local;')
  version = result.one().release_version
  logging.info('Cassandra version: %s', version)
  return session


@pytest.fixture(scope='function', autouse=True)
def setup_data(cassandra_session):
  key_spaces = cassandra_session.execute('desc keyspaces')
  for key_space in key_spaces:
    if not key_space.startswith('system'):
      logging.info('dropping keyspace: %s', key_space)
      cassandra_session.execute(f'drop keyspace {key_space};')
