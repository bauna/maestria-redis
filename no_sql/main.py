import os
import csv

from abc import ABC
from abc import abstractmethod
from redis import Redis


class ImporterCSV(ABC):

  @abstractmethod
  def connect(self):
    pass

  def initialize(self):
    pass

  def process_csv(self, file):
    self.connect()
    try:
      self.initialize()
      with open(file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
          print(f'processing: {row}')
          self.process_row(row)

    finally:
      self.close()

  @abstractmethod
  def process_row(self, row):
    pass

  @abstractmethod
  def close(self):
    pass
