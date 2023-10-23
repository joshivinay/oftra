from dataclasses import dataclass
from typing import Dict, Protocol, Any
from pyspark.sql import DataFrame
from .sql_processor import SqlProcessor
from oftra import ApplicationContext


class Processor(Protocol):

  def __init__(self) -> None:
    super().__init__()

  def create(node: Dict[str,Any]) -> 'Processor':
    if (node['type'] == 'SqlProcessor'):
      return SqlProcessor(**node)
    else:
      raise Exception(f'Unknown nodeType: {node["nodeType"]}')

  def execute(self, context: ApplicationContext) -> DataFrame:
    ...

Processor.create = staticmethod(Processor.create)
  