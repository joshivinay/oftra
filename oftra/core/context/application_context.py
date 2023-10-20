from abc import ABC, abstractmethod
from typing import Dict, Any
from oftra.core.config.oftra_config import OftraConfig

import json

class ApplicationContext(ABC):

  def __new__(cls, config: OftraConfig) -> 'ApplicationContext':
    if not hasattr(cls, 'instance'):
      cls.instance = super(ApplicationContext, cls).__new__(cls)    
    return cls.instance    
  
  def __init__(self, config: OftraConfig) -> None:        
    self.context_holder : Dict[str, Any] = {}
    self.config : OftraConfig = config    

  def get_from_context(self, key: str) -> Any:
    return self.context_holder.get(key) or None
  
  def get(self, key: str) -> Any:
    return self.get(key)
  
