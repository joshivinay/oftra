from dataclasses import dataclass, field
from typing import Dict, Optional, Any, Union
from oftra import ApplicationContext
from oftra.spark.workflow.source.source import Source
from pyspark.sql import DataFrame, DataFrameReader
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import StructType
from pyspark.sql.types import _parse_datatype_json_string


@dataclass
class DeltaSource(Source):
  
    def with_reader_schema(self, context: ApplicationContext, reader: Union[DataFrameReader, DataStreamReader]) -> Union[DataFrameReader, DataStreamReader]:
      return reader    
    
  