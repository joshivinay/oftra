from pyspark import  SparkContext
from pyspark.sql import SparkSession

class OftraContext:
  
  """This is the main singleton application context which contains the spark session and other configuration"""

  def __new__(cls) -> 'OftraContext':
    if not hasattr(cls, 'instance'):
      cls.instance = super(OftraContext, cls).__new__(cls)    
    return cls.instance
  
  def __init__(self, config_file: str) -> None:
    self.config = self.loadConfiguration(config_file)
    self.sparkSession = self.initSparkSession()
    self.sc = self.sparkSession.sparkContext    

  
  def loadConfiguration(self, config_file) -> OftraConfig:
    """Loads the configuration from the given file"""
    with open(config_file) as f:
      self.config = json.load(f)
    return OftraConfig(config_file)


  def initSparkSession(self) -> SparkSession:
    appName = self.config
    SparkSession.builder.appName("")

