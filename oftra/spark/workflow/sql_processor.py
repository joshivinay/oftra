from dataclasses import dataclass, field
from typing import Dict, Optional, List, Any
from oftra import ApplicationContext, Node
from pyspark.sql import DataFrame

@dataclass
class SqlProcessor(Node):  
  
  def execute(self, context: ApplicationContext) -> DataFrame:
     print(f"executing sql processor '{self.name}'")
     sql = self.properties['sql']
     df = context.get_from_context('spark_session').sql(sql)
     df.createOrReplaceTempView(self.name)
     return df

  
  