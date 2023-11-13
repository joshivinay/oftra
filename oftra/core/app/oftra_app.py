from argparse import ArgumentParser
from abc import abstractmethod, ABC
from typing import TypeVar
from oftra import OftraConfig, ApplicationContext, Workflow

T = TypeVar("T", bound="OftraApp")

class OftraApp(ABC):
  def __init__(self) -> None:
    pass

  @abstractmethod
  def create_context(self: T, config: OftraConfig) -> ApplicationContext:
    ...
  
  @abstractmethod
  def create_config(self: T, config_file: str) -> OftraConfig:
   ...

  @abstractmethod
  def create_workflow(self: T, config: OftraConfig, workflow_file: str) -> Workflow:
    ...

  def run(self: T):

    # Parse the command line arguments
    parser = ArgumentParser() 
    parser.add_argument("-W", "--workflow-file", help="Workflow DAG file to execute", default=None, metavar="FILE")
    parser.add_argument("-C", "--config-file", help="External Variables and configurations as file", default=None, metavar="FILE")
    args = parser.parse_args()
    
    
    # Create the config object from the concrete implementation   
    config: OftraConfig = self.create_config(args.config_file)

    # Create the application context. Depending on the type of configuration, create the appropriate application context
    # TODO: Cant instantiate a concrete application context like SparkApplicationContext here.
    context: ApplicationContext = self.create_context(config)    
        
    # Check if the workflow file is provided
    if args.workflow_file is None:
      print("Workflow file is not provided")
      return

    # Create a workflow of the appropriate type
    app_workflow: Workflow = self.create_workflow(config, args.workflow_file)                                  

    # Execute the workflow DAG
    app_workflow.execute(context)

    # If stream mode, then awaitTermination() 
    batch_mode = config.get_as_boolean("batchMode")
    if batch_mode == False:
      context.spark_session.streams.awaitAnyTermination()
    

    
def main():
  app = OftraApp()  
  app.run()

if __name__ == "__main__":
  main()

