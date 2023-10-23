from dataclasses import field
from typing import Any, Dict, List, Optional
from oftra.context.oftra_context import OftraContext
from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, StructType
class ExplodeColumnsProcessor:
    
    name: str  
    type: str
    nodeType: str
    description: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    dependsOn: List[str] = field(default_factory=list)
    properties: Dict[str,str] = field(default_factory=dict)

    def execute(self, context: OftraContext) -> DataFrame:
        print(f"executing explode columns processor '{self.name}'")
        sql = self.properties['sql']
        df = context.sparkSession.sql(sql)
        df.createOrReplaceTempView(self.name)
        return df
    
    # def flatten(fields: List[StructField], sql: str, rem: Optional[str]) -> (List[StructField], str):
        
    #     remaining_columns = ""
    #     if not rem and rem != "":
    #         remaining_columns = rem
        
    #     fields
