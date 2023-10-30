from argparse import ArgumentParser
from oftra.spark.spark_application_context import SparkApplicationContext
from oftra import OftraConfig, ApplicationContext
from oftra.spark.spark_workflow import SparkWorkflow


class OftraApp:
  def __init__(self) -> None:
    pass

  def run(self):

    # Parse the command line arguments
    parser = ArgumentParser() 
    parser.add_argument("-W", "--workflow-file", help="Workflow DAG file to execute", default=None, metavar="FILE")
    parser.add_argument("-C", "--config-file", help="External Variables and configurations as file", default=None, metavar="FILE")
    args = parser.parse_args()
    
    
    # Create the config object    
    config = OftraConfig(args.config_file)

    # Create the application context. Depending on the type of configuration, create the appropriate application context

    context: ApplicationContext = SparkApplicationContext(config)
        
    # Check if the workflow file is provided
    if args.workflow_file is None:
      print("Workflow file is not provided")
      return

    # Create a workflow of the appropriate type
    app_workflow = SparkWorkflow(config, args.workflow_file)

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

