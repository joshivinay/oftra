from dataclasses import dataclass, field
import json
from typing import Any, Dict, Optional
from typing import List
from typing import Protocol

from data.sink import Sink
from data.source import Source
from data.processor import Processor
from config import oftra_config
from data.component import Component
from oftra.context.oftra_context import OftraContext


@dataclass
class Workflow:

  workflow_file: str = field(init=True)
  workflow: List[Component] = field(init=True, default_factory=list)  
  metadata: Dict[str,Any] = Optional[Dict]
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
      for comp in workflow['workflow']:
        if comp['nodeType'] == 'source':
          self.sources.append(Source.create(comp))
        elif comp['nodeType'] == 'processor':
          self.processors.append(Processor.create(comp))
        else:
          self.sinks.append(Sink.create(comp))


  def execute(self, context: OftraContext) -> None:
    
    # Execute each source
    for source in self.sources:
      source.execute(context)

    # Execute each Processor
    for processor in self.processors:
      processor.execute(context)

    # Execute each sink
    for siink in self.sinks:
      siink.execute(context)



    

  
    
  