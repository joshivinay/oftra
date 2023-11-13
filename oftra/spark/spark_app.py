from oftra import OftraConfig
from oftra import OftraApp
from oftra.spark.spark_application_context import SparkApplicationContext
from oftra.spark.spark_workflow import SparkWorkflow

class SparkApp(OftraApp):

  def __init__(self):
    super().__init__()

  def create_config(self, config_file: str) -> OftraConfig:
    self.config = OftraConfig(config_file)
    return self.config
  
  def create_context(self, config: OftraConfig) -> SparkApplicationContext:
    return SparkApplicationContext(config)
  
  def create_workflow(self, config: OftraConfig, workflow_file: str) -> SparkWorkflow:
    return SparkWorkflow(config, workflow_file)