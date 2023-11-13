from dataclasses import dataclass, field
from typing import Dict, Optional, Any, Union
from oftra import ApplicationContext
from oftra.spark.workflow.source.source import Source
from oftra.spark.utils.schema_utils import SchemaUtils
from pyspark.sql import DataFrame, DataFrameReader
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import StructType
from pyspark.sql.types import _parse_datatype_json_string
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import col


@dataclass
class CSVSource(Source):
      
  def with_reader_schema(self, context: ApplicationContext, reader: Union[DataFrameReader, DataStreamReader]) -> Union[DataFrameReader, DataStreamReader]:
        
    read_schema: Optional[StructType] = self.get_schema(context, self.properties.get('schema'))

    if read_schema is not None:
      return reader.schema(read_schema)
    return reader  
  
  def get_schema(self, schema: str) -> Optional[StructType]:
    
    if schema.strip().lower().endswith('.ddl'):
      return SchemaUtils.get_schema_from_ddl(schema)
    else:
      return SchemaUtils.get_schema_from_json(schema)
  
