from dataclasses import dataclass, field
from typing import Optional, Dict

@dataclass
class Component:
  name: str = field(init=True)
  description: Optional[str] = field(init=True)
  nodeType: str = field(init=True)
  type: str = field(init=True)
  properties: Dict[str,str] = field(default_factory=dict)
