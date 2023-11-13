from __future__ import annotations
import json
from typing import Any, Dict
from oftra.core.config.config_reader import ConfigReader


class OftraConfig:
    
     """
     Configuration for the framework. This framework can take in arguments of the format
     --workflow <workflowFile.json> --config <configFile> where the workflow.json file is the DAG containing the workflow
     and the configFile is the configuration file with variable / parameters for the framework.
     """
     _conf : Dict[str,Any] = {}

     def __init__(self, config_file) -> None:                  
         self.config_file = config_file
         if config_file is not None:
            config_reader: ConfigReader = ConfigReader.create(config_file)            
            self._conf =  config_reader.read(config_file)            



     def get_config(self):
         return self._conf
     
     def get(self, key: str) -> Any:
         return self._conf.get(key.strip())
     
     def get_as_boolean(self, key:str) -> bool:
         value = self._conf.get(key.strip())
         if value is None or value.lower() == 'false':
             return False
         elif value.lower() == 'true':
             return True     
         else:
             return False
          
     def get_as_integer(self, key:str) -> int:
         return int(self._conf.get(key.strip()))
     
     def get_as_string(self, key:str) -> str:
         return str(self._conf.get(key.strip()))
