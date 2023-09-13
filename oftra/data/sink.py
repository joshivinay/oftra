from dataclasses import dataclass
from typing import Dict, Protocol

from data.delta_sink import DeltaSink



class Sink(Protocol):
  
  def __init__(self) -> None:
    super().__init__()

  def create(json: Dict[str,str]) -> 'Sink':
    if (json['nodeType'] == 'DeltaSink'):
      return DeltaSink(**json)
    else:
      raise Exception(f'Unknown nodeType: {json["nodeType"]}')

  def execute(self):
    ...

Sink.create = staticmethod(Sink.create)
  