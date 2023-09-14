from dataclasses import dataclass, field
from typing import Dict, Optional, List, Any, Union
from oftra.context.oftra_context import OftraContext
from pyspark.sql import DataFrame, DataFrameWriter
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery


@dataclass
class DeltaSink:
  name: str  
  type: str
  nodeType: str
  description: Optional[str] = None
  metadata: Optional[Dict[str, Any]] = None  
  properties: Dict[str,str] = field(default_factory=dict)
  
  
  def execute(self, context: OftraContext) -> DataFrame:
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
        
  def fetch_df_to_write(self, context: OftraContext) -> DataFrame:
     
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

     df = context.sparkSession.sql(sql)
     return df
  
  def writer_with_format(self, context: OftraContext, df: DataFrame) -> Union[DataFrameWriter, DataStreamWriter]:
     format = self.properties.get('format')
     batch_mode = context.config.get('batch_mode') or True
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

  def write_df(self, context: OftraContext, writer:Union[DataFrameWriter, DataStreamWriter]) -> Union[DataFrameWriter, DataStreamWriter]:
      if type(writer) == DataFrameWriter:
        self.write_batch(context, writer)
      else:
        self.write_stream(context, writer)

  def write_batch(self, context: OftraContext, writer: DataFrameWriter):
    path = self.properties.get('savePath')
    if path is None:
      writer.save()
    else:
      writer.save(path)

  def write_stream(self, context: OftraContext, writer: DataStreamWriter) -> StreamingQuery:
    path = self.properties.get('path')
    trigger_type = self.properties['trigger']
    
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
