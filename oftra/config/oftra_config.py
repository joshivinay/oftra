from __future__ import annotations
import json
from typing import Any, Dict

class OftraConfig:
    
     """
     Configuration for the framework. This framework can take in arguments of the format
     --workflow <workflowFile.json> --config <configFile> where the workflow.json file is the DAG containing the workflow
     and the configFile is the configuration file with variable / parameters for the framework.
     """
     _conf : Dict[str,Any] = {}

     def __init__(self, config_file):
         
         self.config_file = config_file
         if config_file is not None:
            with open(config_file) as f:
                self._conf = json.loads(f.read())                 
         
     
     def get_config(self):
         return self._conf
     def get(self, key: str) -> Any:
         return self._conf.get(key)
