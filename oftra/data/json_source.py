from dataclasses import dataclass, field
from typing import Dict, Optional, Any, Union
from oftra.context.oftra_context import OftraContext
from pyspark.sql import DataFrame, DataFrameReader
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import StructType

@dataclass
class JsonSource:
  name: str  
  type: str
  nodeType: str
  description: Optional[str] = None
  metadata: Optional[Dict[str, Any]] = None
  properties: Dict[str,str] = field(default_factory=dict)
  
  
  def execute(self, context: OftraContext) -> DataFrame:
     print(f"executing json source '{self.name}'")
     df_reader = self.create_reader(context)
     df_reader_with_schema = self.with_reader_schema(context, df_reader)
     df_reader_with_options = self.with_options(context, df_reader_with_schema)
     df = self.create_source_or_table(context, df_reader_with_options)
     partitioned_df = self.partition_df(context, df)
     filtered_df = self.filter_df(context, partitioned_df)
     enriched_df = self.enrich(context, filtered_df)
     final_df = self.post_process(context, enriched_df)
     final_df.createOrReplaceTempView(self.name)
     return final_df

  def create_reader(self, context: OftraContext) -> Union[DataFrameReader, DataStreamReader]:
     
    format = self.properties['format']
    load_path = self.properties.get('loadPath')
    spark = context.sparkSession
    batch_mode = context.config.get('batch_mode') or True
    if batch_mode:
      return spark.read.format(format)
    else:
      return spark.readStream.format(format)
    
  def with_reader_schema(self, context: OftraContext, reader: Union[DataFrameReader, DataStreamReader]) -> Union[DataFrameReader, DataStreamReader]:
        
    read_schema: Optional[StructType] = self.get_schema(context, self.properties.get('schema'))

    if read_schema is not None:
      return reader.schema(read_schema)
    return reader  

  def get_schema(self, context: OftraContext, schema: str) -> Optional[StructType]:
    return None
  
  def with_options(self, context: OftraContext, reader: Union[DataFrameReader, DataStreamReader]) -> Union[DataFrameReader, DataStreamReader]:
    options = self.properties
    if options is None:
      return reader
        
    return reader.options(**options)
  
  def create_source_or_table(self, context: OftraContext, reader: Union[DataFrameReader, DataStreamReader]) -> DataFrame:
    
    type = self.type
    path = self.properties['loadPath']
    batch_mode = context.config.get('batch_mode') or True
    if type == 'table' and batch_mode:
      return reader.table(self.properties['table'])
    elif batch_mode is False:
      raise Exception("Stream mode doesn't support tables")
    
    if path is None:
      return reader.load()
    else:
      return reader.load(path)

  def partition_df(self, context: OftraContext, df: DataFrame) -> DataFrame:
    return df
  
  def filter_df(self, context: OftraContext, df: DataFrame) -> DataFrame:
    return df
  
  def enrich(self, context: OftraContext, df: DataFrame) -> DataFrame:
    return df
  
  def post_process(self, context: OftraContext, df: DataFrame) -> DataFrame:
    return df
  
  