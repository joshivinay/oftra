from oftra import ApplicationContext
from oftra import OftraConfig
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

class SparkApplicationContext(ApplicationContext):
  def __init__(self, config: OftraConfig):
    
    # Set the config in the base class
    super().__init__(config)

    # Initialize the spark session
    self.spark_session = self._initSparkSession()        
    self.sc = self.spark_session.sparkContext        

    # Set the values in the context holder
    self.context_holder["spark_session"] = self.spark_session
    self.context_holder["spark_context"] = self.sc


  def _initSparkSession(self) -> SparkSession:
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
  
  def spark_session(self) -> SparkSession:
    return self.get("sparkSession")
  
  def spark_context(self) -> SparkContext:
    return self.get("sparkContext")