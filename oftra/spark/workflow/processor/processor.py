from dataclasses import dataclass
from typing import Dict, Protocol, Any, TypeVar, Union
from pyspark.sql import DataFrame
from oftra import ApplicationContext
from oftra import Node

T = TypeVar("T", bound="Processor")

@dataclass
class Processor(Node):


  def create(node: Dict[str,Any]) -> 'Processor':
    from oftra.spark.workflow.workflow_factory import processor_factory
    return processor_factory(node)

  def execute(self: T, context: ApplicationContext) -> DataFrame:
    print(f"executing processor '{self.name}'")
    sql = self.properties['sql']
    df = context.get_from_context('spark_session').sql(sql)
    df.createOrReplaceTempView(self.name)
    return df


Processor.create = staticmethod(Processor.create)
  