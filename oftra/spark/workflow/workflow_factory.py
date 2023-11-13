from oftra.spark.workflow.source import Source
from oftra.spark.workflow.csv_source import CSVSource
from oftra.spark.workflow.delta_source import DeltaSource
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
