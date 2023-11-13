from dataclasses import dataclass, field
import json
from typing import Any, Dict, Optional
from typing import List
from typing import Protocol
from oftra.core.workflow.workflow import Workflow
from oftra.spark.workflow.source.source import Source
from oftra.spark.workflow.processor.processor import Processor
from oftra.spark.workflow.sink.sink import Sink
from oftra import ApplicationContext

@dataclass
class SparkWorkflow(Workflow):
  
  '''
  A Workflow is a DAG of nodes. Each type of workflow (in this case, SparkWorkflow) knows 
  how to create itself via implementation of the load() method.
  TODO: Deserialize the workflow file into a Workflow object directly. 
  '''
  def load(self, workflow_file: str) -> Dict[str,Any]:
    with open(workflow_file, "r") as f:
      workflow_json = json.loads(f.read())
      for node in workflow_json['workflow']:        
        if node['nodeType'] =='source':
          self.workflow[node['name']] = Source.create(node)
        elif node['nodeType'] == 'processor':
          self.workflow[node['name']] = Processor.create(node)
        else:          
          self.workflow[node['name']] = Sink.create(node)
        
      self.metadata = workflow_json['metadata']


  def execute(self, context: ApplicationContext) -> None:
    for node in self.dag():
      self.workflow.get(node).execute(context)


    

  
    
  