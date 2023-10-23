from typing import Protocol, Optional, Any
import os
class ConfigProvider(Protocol):

  def get(self, key: str) -> Optional[Any]:
    ...

class EnvProvider:
  def get(self, key: str) -> Optional[Any]:
    return os.environ.get(key)

class DictProvider:
  def __init__(self, config: dict):
    self.config = config

  def get(self, key: str) -> Optional[Any]:
    return self.config.get(key)



