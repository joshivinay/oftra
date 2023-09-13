from dataclasses import dataclass
from typing import Dict, Protocol

from data.sql_processor import SqlProcessor



class Processor(Protocol):

  def __init__(self) -> None:
    super().__init__()

  def create(json: Dict[str,str]) -> 'Processor':
    if (json['nodeType'] == 'SqlProcessor'):
      return SqlProcessor(**json)
    else:
      raise Exception(f'Unknown nodeType: {json["nodeType"]}')

  def execute(self):
    ...

Processor.create = staticmethod(Processor.create)
  