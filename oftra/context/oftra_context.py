from pyspark import  SparkContext, SparkConf
from pyspark.sql import SparkSession
from config.oftra_config import OftraConfig
import json

class OftraContext:
  
  """This is the main singleton application context which contains the spark session and other configuration"""

  def __new__(cls, config_file) -> 'OftraContext':
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
    appName = self.config.get('appName') or 'OftraSparkApplication'
    master = self.config.get('master') or 'local[*]'
    sparkConf = SparkConf()
    sparkConf.setAppName(appName).setMaster(master)

    # sparkConf = self.config.get('spark') or SparkConf()
    sparkConfProps = self.config.get('sparkConf')
    if sparkConfProps is not None:
      confs = [(k, v) for k, v in sparkConfProps.items()]
      sparkConf.setAll(confs)

    spark = SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()
    # spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

