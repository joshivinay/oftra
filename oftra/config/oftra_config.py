from __future__ import annotations
from configparser import ConfigParser
from typing import Dict

class OftraConfig:
     """
     Configuration for the framework. This framework can take in arguments of the format
     --workflow <workflowFile.json> --config <configFile> where the workflow.json file is the DAG containing the workflow
     and the configFile is the configuration file with variable / parameters for the framework.
     """
     _conf : Dict[str,str] = {}

     def __init__(self, config_file):
         self.config_file = config_file
         self.config = ConfigParser()
         if config_file is not None:
            self.config.read(config_file)

    
     def get_config(self):
         return self.config
