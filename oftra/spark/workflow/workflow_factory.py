from oftra.spark.workflow.source.source import Source
from oftra.spark.workflow.source.csv_source import CSVSource
from oftra.spark.workflow.source.delta_source import DeltaSource
from oftra.spark.workflow.processor.processor import Processor
from oftra.spark.workflow.sink.sink import Sink

from typing import Dict, Any

def source_factory(node: Dict[str, Any]) -> Source:
    if (node['type'] == 'JsonSource'):
      source = Source(**node)
      return source
    elif (node['type'] == 'CSVSource'):
      source = CSVSource(**node)
      return source
    elif (node['type'] == 'DeltaSource'):
      source = DeltaSource(**node)
      return source
    else:
      raise Exception(f"Unknown nodeType: {node['nodeType']}")
    
def processor_factory(node: Dict[str, Any]) -> Processor:
    if (node['type'] == 'SqlProcessor'):
      return Processor(**node)
    else:
      raise Exception(f'Unknown nodeType: {node["nodeType"]}')

def sink_factory(node: Dict[str, Any]) -> Sink:
  if (node['type'] == 'DeltaSink'):
    return Sink(**node)
  else:
    raise Exception(f'Unknown nodeType: {node["nodeType"]}')