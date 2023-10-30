from typing import Dict, Protocol, Any
from .json_source import JsonSource
from .delta_source import DeltaSource
from pyspark.sql import DataFrame
from oftra import ApplicationContext
from oftra import Node

class Source(Protocol):
  
  def __init__(self, node: Node) -> None:
    super().__init__()  
  
  def create(node: Dict[str, Any]) -> 'Source':
    if (node['type'] == 'JsonSource'):
      source = JsonSource(**node)
      return source
    elif (node['type'] == 'DeltaSource'):
      source = DeltaSource(**node)
      return source
    else:
      raise Exception(f"Unknown nodeType: {node['nodeType']}")
    
  def execute(self, context: ApplicationContext) -> DataFrame:
     ...

Source.create = staticmethod(Source.create)
  