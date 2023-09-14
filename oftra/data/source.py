from typing import Dict, Protocol
from data.json_source import JsonSource
from pyspark.sql import DataFrame
from oftra.context.oftra_context import OftraContext

class Source(Protocol):
    
  def __init__(self) -> None:
    super().__init__()  

  def create(json: Dict[str,str]) -> 'Source':
    if (json['type'] == 'JsonSource'):
      jsonSource = JsonSource(**json)
      return jsonSource
    else:
      raise Exception(f'Unknown nodeType: {json["nodeType"]}')
  
  
  def execute(self) -> None:
     ...

Source.create = staticmethod(Source.create)
  