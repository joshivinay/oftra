from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from abc import abstractmethod

@dataclass(kw_only=True)
class Node:
  name: str = field(init=True)
  description: Optional[str] = None
  nodeType: str = field(init=True)
  type: str = field(init=True)
  properties: Dict[str,Any] = field(default_factory=dict)
  metadata: Dict[str,Any] = field(default_factory=dict)
