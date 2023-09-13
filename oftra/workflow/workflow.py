from dataclasses import dataclass, field
import json
from typing import Dict
from typing import List
from typing import Protocol

from data.sink import Sink
from data.source import Source
from data.processor import Processor
from config import oftra_config



@dataclass
class Workflow:

  workflow_file: str = field(init=True)
  sources: List[Source] = field(init=True, default_factory=list)
  processors: List[Processor] = field(init=True, default_factory=list)
  sinks: List[Sink] = field(init=True, default_factory=list)
  

  def __post_init__(self) -> None:
    if self.workflow_file:
      self.load_workflow(self.workflow_file)

  def load_workflow(self, workflow_file: str) -> None:
    # the workflow file is a list of nodes. Read each list and instantiate the sources, processors and sinks
    with open(workflow_file) as f:
      workflow = json.loads(f.read())    
      for source in workflow['sources']:                
        self.sources.append(Source.create(source))
      for processor in workflow['processors']:
        self.processors.append(Processor.create(processor))
      for sink in workflow['sinks']:
        self.sinks.append(Sink.create(sink))

  def execute(self, config: oftra_config.OftraConfig) -> None:
    
    # Execute each source
    for source in self.sources:
      source.execute()

    # Execute each Processor
    for processor in self.processors:
      processor.execute()

    # Execute each sink
    for siink in self.sinks:
      siink.execute()



    

  
    
  