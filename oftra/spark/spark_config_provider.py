from typing import Dict, Any, Optional

class SparkConfigProvider:

  def __init__(self, config: Dict[str,Any]) -> None:
    self.config = config

  def get(self, key: str) -> Optional[Any]:
    if (key.startsWith("spark.")):
      return self.config.get(key)
    else:
      return None
