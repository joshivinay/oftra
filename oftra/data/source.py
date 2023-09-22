from typing import Dict, Protocol
from data.json_source import JsonSource
from oftra.data.delta_source import DeltaSource
from pyspark.sql import DataFrame
from oftra.context.oftra_context import OftraContext

class Source(Protocol):
    
  def __init__(self) -> None:
    super().__init__()  

  def create(json: Dict[str,str]) -> 'Source':
    if (json['type'] == 'JsonSource'):
      source = JsonSource(**json)
      return source
    elif (json['type'] == 'DeltaSource'):
      source = DeltaSource(**json)
      return source
    else:
      raise Exception(f'Unknown nodeType: {json["nodeType"]}')
  
  
  def execute(self) -> None:
     ...

Source.create = staticmethod(Source.create)
  