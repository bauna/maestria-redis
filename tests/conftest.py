import os
import logging
import pytest

logging.basicConfig(level=logging.DEBUG)

@pytest.fixture(scope='session', autouse=True)
def base_path():
  return os.getcwd()