from argparse import ArgumentParser
from config import oftra_config
from oftra.context.oftra_context import OftraContext
from workflow import workflow


class OftraApp:
  def __init__(self) -> None:
    pass

  def run(self):

    # Parse the command line arguments
    parser = ArgumentParser() 
    parser.add_argument("-W", "--workflow-file", help="Workflow DAG file to execute", default=None, metavar="FILE")
    parser.add_argument("-C", "--config-file", help="External Variables and configurations as file", default=None, metavar="FILE")
    args = parser.parse_args()

    # Create the context    
    context = OftraContext(args.config_file)
        
    # Check if the workflow file is provided
    if args.workflow_file is None:
      print("Workflow file is not provided")
      return

    # Load the workflow DAG from the workflow file. In future it could be a workflow string
    workflow1 = workflow.Workflow(args.workflow_file)

    # Load the external variables and configurations into a configuration object
    # config = oftra_config.OftraConfig(args.config_file)

    # Execute the workflow DAG
    workflow1.execute(context)
    


if __name__ == "__main__":
  app = OftraApp()  
  app.run()
