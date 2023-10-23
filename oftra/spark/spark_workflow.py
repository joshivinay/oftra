from dataclasses import dataclass, field
import json
from typing import Any, Dict, Optional
from typing import List
from typing import Protocol
from oftra.core.workflow.workflow import Workflow
from oftra.spark.workflow import Source, Processor, Sink
from oftra import ApplicationContext

@dataclass
class SparkWorkflow(Workflow):
  
  
  def load(self, workflow_file: str) -> Dict[str,Any]:
    with open(workflow_file, "r") as f:
      workflow = json.loads(f.read())
      for node in workflow['workflow']:        
        if node['nodeType'] =='source':
          self.workflow[node['name']] = Source.create(node)
        elif node['nodeType'] == 'processor':
          self.workflow[node['name']] = Processor.create(node)
        else:          
          self.workflow[node['name']] = Sink.create(node)
        
      self.metadata = workflow['metadata']


  def execute(self, context: ApplicationContext) -> None:
    for node in self.dag():
      self.workflow.get(node).execute(context)

    # for node in self.dag():
    #   if node.nodeType == 'source':
    #     source = Source.create(node)
    #     source.execute(context)
    #   elif node.nodeType == 'processor':
    #     processor = Processor.create(node)
    #     processor.execute(context)
    #   else:
    #     sink = Sink.create(node)
    #     sink.execute(context)


    

  
    
  