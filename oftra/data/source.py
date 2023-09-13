from typing import Dict, Protocol
from data.json_source import JsonSource


class Source(Protocol):
    
  def __init__(self) -> None:
    super().__init__()  

  def create(json: Dict[str,str]) -> 'Source':
    if (json['nodeType'] == 'JsonSource'):
      jsonSource = JsonSource(**json)
      return jsonSource
    else:
      raise Exception(f'Unknown nodeType: {json["nodeType"]}')
  
  def execute(self) -> None:
     ...

Source.create = staticmethod(Source.create)
    
  