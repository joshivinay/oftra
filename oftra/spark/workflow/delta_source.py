from dataclasses import dataclass, field
from typing import Dict, Optional, Any, Union
from oftra import ApplicationContext
from oftra import Node
from pyspark.sql import DataFrame, DataFrameReader
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import StructType
from pyspark.sql.types import _parse_datatype_json_string


@dataclass
class DeltaSource(Node):
  
  def execute(self, context: ApplicationContext) -> DataFrame:
     print(f"executing delta source '{self.name}'")
     df_reader = self.create_reader(context)
     df_reader_with_options = self.with_options(context, df_reader)
     df = self.create_source_or_table(context, df_reader_with_options)
     partitioned_df = self.partition_df(context, df)
     filtered_df = self.filter_df(context, partitioned_df)
     enriched_df = self.enrich(context, filtered_df)
     final_df = self.post_process(context, enriched_df)
     final_df.createOrReplaceTempView(self.name)
     return final_df

  def create_reader(self, context: ApplicationContext) -> Union[DataFrameReader, DataStreamReader]:
     
    format = self.properties['format']
    load_path = self.properties.get('loadPath')
    spark = context.spark_session
    batch_mode = context.config.get_as_boolean('batchMode')
    if batch_mode:
      return spark.read.format(format)
    else:
      return spark.readStream.format(format)
      
  def with_options(self, context: ApplicationContext, reader: Union[DataFrameReader, DataStreamReader]) -> Union[DataFrameReader, DataStreamReader]:
    options = self.properties
    if options is None:
      return reader
        
    return reader.options(**options)
  
  def create_source_or_table(self, context: ApplicationContext, reader: Union[DataFrameReader, DataStreamReader]) -> DataFrame:
    
    type = self.type
    path = self.properties['loadPath']
    batch_mode = context.config.get_as_boolean('batchMode')
    if type == 'table':
      if batch_mode is True:
        return reader.table(self.properties['table'])
      else:
        raise Exception("Stream mode doesn't support tables")      
    
    if path is None:
      return reader.load()
    else:
      return reader.load(path)

  def partition_df(self, context: ApplicationContext, df: DataFrame) -> DataFrame:
    return df
  
  def filter_df(self, context: ApplicationContext, df: DataFrame) -> DataFrame:
    return df
  
  def enrich(self, context: ApplicationContext, df: DataFrame) -> DataFrame:
    return df
  
  def post_process(self, context: ApplicationContext, df: DataFrame) -> DataFrame:
    return df
  
  