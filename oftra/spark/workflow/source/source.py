from typing import Dict, Protocol, Any, TypeVar
from pyspark.sql import DataFrame
from dataclasses import dataclass, field
from typing import Dict, Optional, Any, Union
from pyspark.sql import DataFrame, DataFrameReader
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import StructType
from pyspark.sql.types import _parse_datatype_json_string
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import col
from oftra import ApplicationContext
from oftra import Node


T = TypeVar("T", bound="Source")

@dataclass
class Source(Node):
    
  def create(node: Dict[str, Any]) -> 'Source':
    from oftra.spark.workflow.workflow_factory import source_factory
    return source_factory(node)
    
  '''
  Executes the source and returns a DataFrame.
  Step 1: Create a Dataframe or DataStreamReader
  Step 2: Apply schema if available
  Step 3: Apply options if available
  Step 4: Create source or table
  Step 5: Partition the dataframe if needed
  Step 6: Filter the dataframe if needed
  Step 7: Enrich the dataframe if needed
  '''
  def execute(self: T, context: ApplicationContext) -> DataFrame:
    print(f"Reading from source '{self.name}'")
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

  def create_reader(self:T, context: ApplicationContext) -> Union[DataFrameReader, DataStreamReader]:
     
    format = self.properties['format']    
    spark = context.get_from_context('spark_session')
    batch_mode = context.config.get_as_boolean('batchMode')
    if batch_mode:
      return spark.read.format(format)
    else:
      return spark.readStream.format(format)
    
    
  def create_source_or_table(self: T, context: ApplicationContext, reader: Union[DataFrameReader, DataStreamReader]) -> DataFrame:
    
    type = self.type
    path = self.properties['loadPath']
    batch_mode = context.config.get_as_boolean('batchMode')
    select = context.config.get('select')
    if select is not None:
      return reader.load(path).select(select)    

    if type == 'table':
      if batch_mode is True:
        return reader.table(self.properties['table'])
      else:
        raise Exception("Stream mode doesn't support tables")      
    
    if path is None:
      return reader.load()
    else:
      return reader.load(path)

  def with_reader_schema(self: T, context: ApplicationContext, reader: Union[DataFrameReader, DataStreamReader]) -> Union[DataFrameReader, DataStreamReader]:
        
    read_schema: Optional[StructType] = self.get_schema(context, self.properties.get('jsonSchema'))

    if read_schema is not None:
      return reader.schema(read_schema)
    return reader  

  def get_schema(self: T, context: ApplicationContext, schema: str) -> Optional[StructType]:
    # This is path to the schema file. Read the file which is in jsonschema format and then convert to Spark schema
    with open(schema) as f:
      schema_as_str = f.read()  
      schema_as_spark_java = context.get_from_context('spark_session')._jvm.org.zalando.spark.jsonschema.SchemaConverter.convertContent(schema_as_str)      
      schema_as_spark = _parse_datatype_json_string(schema_as_spark_java.json())
    return schema_as_spark
  
  def with_options(self: T, context: ApplicationContext, reader: Union[DataFrameReader, DataStreamReader]) -> Union[DataFrameReader, DataStreamReader]:
    options = self.properties
    if options is None:
      return reader
        
    return reader.options(**options)


  def partition_df(self: T, context: ApplicationContext, df: DataFrame) -> DataFrame:
    return df
  
  def filter_df(self: T, context: ApplicationContext, df: DataFrame) -> DataFrame:
    return df
  
  def enrich(self: T, context: ApplicationContext, df: DataFrame) -> DataFrame:
    enrich = self.properties.get("enrich") 
    if enrich is None:
      return df
    else:
      input_file_name = enrich.get("inputFileName")
      if input_file_name is True:
        df = df.withColumn("source_file_name", col("_metadata.file_path"))
        return df
    return df
  
  def post_process(self: T, context: ApplicationContext, df: DataFrame) -> DataFrame:
    return df
  
# Make create as a static method
Source.create = staticmethod(Source.create)
  