from dataclasses import dataclass, field
from typing import Dict, Protocol


@dataclass
class JsonSource:
  name: str  
  nodeType: str
  properties: Dict[str,str] = field(default_factory=dict)
  
  def execute(self) -> None:
     print("executing source {name}")

    
  