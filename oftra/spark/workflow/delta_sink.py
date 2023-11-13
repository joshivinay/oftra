from dataclasses import dataclass, field
from typing import Dict, Optional, List, Any, Union
from oftra import ApplicationContext, Node
from pyspark.sql import DataFrame, DataFrameWriter
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery


@dataclass
class DeltaSink(Node):  
  
  def execute(self, context: ApplicationContext) -> DataFrame:
     print(f"executing delta sink '{self.name}'")
     df_to_write = self.fetch_df_to_write(context)
     df_writer = self.writer_with_format(context, df_to_write)
     df_writer_with_partition = self.writer_with_partition(context, df_writer)
     df_writer_with_options = self.writer_with_options(context, df_writer_with_partition)
     df_sorted_writer = self.writer_with_sort_by(context, df_writer_with_options)
     df_bucketed_writer = self.writer_with_bucket_by(context, df_sorted_writer)
     df_writer_with_save_mode = self.writer_with_mode(context, df_bucketed_writer)     
     self.write_df(context,df_writer_with_save_mode)     
     return df_to_write
        
  def fetch_df_to_write(self, context: ApplicationContext) -> DataFrame:
     
     # Handle select columns
     select = self.properties.get('select') or ' * '
     distinct = self.properties.get('distinct')
     distinct_expr = ''
     if distinct is True:
        distinct_expr = ' DISTINCT '
     sources = ','.join(self.properties['dependsOn'])
     sql = f"SELECT {distinct_expr} {select} FROM {sources}"
     
     where_clause = self.properties.get('where')
     if where_clause is not None:
        sql = sql + f" WHERE {where_clause}"

     print(f"sql for delta sink is '{sql}'")

     df = context.get_from_context('spark_session').sql(sql)
     return df
  
  def writer_with_format(self, context: ApplicationContext, df: DataFrame) -> Union[DataFrameWriter, DataStreamWriter]:
     format = self.properties.get('format')
     batch_mode = context.config.get_as_boolean('batchMode')
     if format is not None:
        if batch_mode:
          return df.write.format(format)
        else:
           return df.writeStream.format(format)
     else:
        if batch_mode:
           return df.write
        else:
          return df.writeStream
     
  def writer_with_partition(self, context, writer: Union[DataFrameWriter, DataStreamWriter]) -> Union[DataFrameWriter, DataStreamWriter]:
    
    partition_by_prop = self.properties.get('partitionBy') or None
    batch_mode = context.config.get_as_boolean('batchMode')

    if partition_by_prop is not None:
      partition_list = [each.strip() for each in partition_by_prop.split(",")]
      are_partitions_defined = partition_list if partition_list else None
    else:
      are_partitions_defined = None

    if are_partitions_defined is not None:
      num_partitions = self.properties.get('numPartitions', None)
      if num_partitions is not None:
        
        return writer.partitionBy(partition_list, num_partitions)
    else:
      return writer
  
  def writer_with_options(self, context, writer: Union[DataFrameWriter, DataStreamWriter]) -> Union[DataFrameWriter, DataStreamWriter]:
    options = self.properties
    if options is None:
      return writer
            
    return writer.options(**options)
  
  def writer_with_sort_by(self, context, writer: Union[DataFrameWriter, DataStreamWriter]) -> Union[DataFrameWriter, DataStreamWriter]:
    return writer
    
  def writer_with_bucket_by(self, context, writer: Union[DataFrameWriter, DataStreamWriter]) -> Union[DataFrameWriter, DataStreamWriter]:
    return writer
  
  def writer_with_mode(self, context, writer: Union[DataFrameWriter, DataStreamWriter]) -> Union[DataFrameWriter, DataStreamWriter]:
    mode = self.properties.get('mode')

    if mode is None:
       return writer
    
    if type(writer) == DataFrameWriter:
      if mode.upper() == "OVERWRITE":
        writer.mode('overwrite')
      elif mode.upper() == "APPEND":
        writer.mode('append') 
      elif mode.upper() == "ERRORIFEXISTS":
        writer.mode('error') 
      else:
        writer.mode('ignore')
    else:
      if mode.upper() == "COMPLETE":
        writer.outputMode('complete')
      elif mode.upper() == "APPEND":
        writer.outputMode('append') 
      elif mode.upper() == "UPDATE":
        writer.outputMode('error') 
      else:
        writer.outputMode('append') 

    return writer

  def write_df(self, context: ApplicationContext, writer:Union[DataFrameWriter, DataStreamWriter]) -> Union[DataFrameWriter, DataStreamWriter]:
      if type(writer) == DataFrameWriter:
        self.write_batch(context, writer)
      else:
        self.write_stream(context, writer)

  def write_batch(self, context: ApplicationContext, writer: DataFrameWriter):
    path = self.properties.get('savePath')
    if path is None:
      writer.save()
    else:
      writer.save(path)

  def write_stream(self, context: ApplicationContext, writer: DataStreamWriter) -> StreamingQuery:
    path = self.properties.get('savePath')
    trigger_type = self.properties.get('trigger')
    
    # If there is no trigger type, default to availableNow
    if (trigger_type == None):
      trigger_type = 'availableNow'
    
    if trigger_type == 'continuous':
      time_interval = self.properties.get('time_interval') or '1 second'
      writer.trigger(continuous=time_interval)
    elif trigger_type == 'fixed':
      time_interval = self.properties.get('time_interval') or '1 second'
      writer.trigger(processingTime=time_interval)
    elif trigger_type == 'once':
      writer.trigger(once=True)
    else:
      writer.trigger(availableNow=True)

    if path is None:
      query = writer.start()        
    else:
      query = writer.start(path)
    
    return query
