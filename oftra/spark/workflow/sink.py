from dataclasses import dataclass
from typing import Dict, Protocol, Any
from .delta_sink import DeltaSink
from oftra import ApplicationContext
from pyspark.sql import DataFrame

class Sink(Protocol):
  
  def __init__(self) -> None:
    super().__init__()

  def create(node: Dict[str,Any]) -> 'Sink':
    if (node['type'] == 'DeltaSink'):
      return DeltaSink(**node)
    else:
      raise Exception(f'Unknown nodeType: {node["nodeType"]}')


  def execute(self, context: ApplicationContext) -> DataFrame:
    ...

Sink.create = staticmethod(Sink.create)
  