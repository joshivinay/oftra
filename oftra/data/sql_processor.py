from dataclasses import dataclass, field
from typing import Dict, Optional, List, Any
from oftra.context.oftra_context import OftraContext
from pyspark.sql import DataFrame

@dataclass
class SqlProcessor:
  name: str  
  type: str
  nodeType: str
  description: Optional[str] = None
  metadata: Optional[Dict[str, Any]] = None
  dependsOn: List[str] = field(default_factory=list)
  properties: Dict[str,str] = field(default_factory=dict)
  
  
  def execute(self, context: OftraContext) -> DataFrame:
     print(f"executing sql processor '{self.name}'")
     sql = self.properties['sql']
     df = context.sparkSession.sql(sql)
     df.createOrReplaceTempView(self.name)
     return df

  
  