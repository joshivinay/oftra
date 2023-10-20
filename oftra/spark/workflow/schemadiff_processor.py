from dataclasses import field
from typing import Any, Dict, List, Optional
from oftra.context.oftra_context import OftraContext
from pyspark.sql import DataFrame
class SchemaDiffProcessor:
  name: str  
  type: str
  nodeType: str
  description: Optional[str] = None
  metadata: Optional[Dict[str, Any]] = None
  dependsOn: List[str] = field(default_factory=list)
  properties: Dict[str,str] = field(default_factory=dict)
  
  def execute(self, context: OftraContext) -> DataFrame:
    return None
