import os
import pytest

from testcontainers.redis import RedisContainer

@pytest.fixture(scope='session', autouse=True)
def redis_container(request):
    redis_container = RedisContainer()
    request.addfinalizer(redis_container.stop)
    redis_container.start()

    os.environ['REDIS_HOST'] = redis_container.get_container_host_ip()
    os.environ['REDIS_PORT'] = redis_container.get_exposed_port(6379)
    return redis_container

@pytest.fixture(scope='session', autouse=True)
def redis_client(redis_container):
    redis_client = redis_container.get_client()
    return redis_client

@pytest.fixture(scope='function', autouse=True)
def setup_data(redis_client):
    redis_client.flushall()
