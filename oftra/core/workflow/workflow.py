from abc import abstractmethod, ABC
from dataclasses import dataclass, field
import json
import networkx as nx
from typing import Any, Dict, Optional
from typing import List
from typing import Protocol
from oftra.core.workflow.node import Node
from oftra import ApplicationContext
from oftra import OftraConfig


@dataclass
class Workflow(ABC):

  config: OftraConfig = field(init=True)
  workflow_file: str = field(init=True)  

  workflow: Dict[str,Node] = field(init=True, default_factory=dict)  
  # edges: List[DirectedEdge] = field(init=True, default_factory=list)  
  metadata: Dict[str,Any] = Optional[Dict]
  topo_sorted_dag: List[str] = field(init=True, default_factory=list)  

  def __post_init__(self):    
    if (self.workflow_file):
      self.load(self.workflow_file)
      # self.load_workflow(self.workflow_file)

    G = nx.DiGraph()
    for name, node in self.workflow.items():
      G.add_node(name)
      for dependency in node.properties.get("dependsOn", []):
        dependentNode = self.workflow.get(dependency)
        G.add_edge(dependentNode.name, name)

    if not nx.is_directed_acyclic_graph(G):
      raise Exception("Cycle detected. The workflow is not valid.")
    else:
      self.topo_sorted_dag = list(nx.topological_sort(G))


  def dag(self) -> List[str]:
    return self.topo_sorted_dag
    
  # def load_workflow(self, workflow_file: str):
  #   with open(workflow_file, "r") as f:
  #     workflow = json.loads(f.read())
  #     for node in workflow['workflow']:
  #       self.workflow.append(Node(**node))      
        
  #     self.metadata = workflow['metadata']
    

  @abstractmethod
  def load(self, workflow_file: str) -> Dict[str, Node]:
    ...

  @abstractmethod
  def execute(self, context: ApplicationContext):
    ...