from typing import Protocol, Dict, Any
import yaml
import json

class ConfigReader(Protocol):
    """
    An interface for reading configuration data from various formats.

    This interface defines a common contract for classes that read and parse
    configuration data from different file formats like JSON, YAML, or XML.

    Attributes:
        None

    Methods:
        read(config_file_path: str) -> Dict[str, Any]:
            Reads and parses the configuration data from a file and returns it
            as a dictionary.

    Example:
        >>> class JSONConfigReader(ConfigReader):
        ...     def read(self, config_file_path: str) -> Dict[str, Any]:
        ...         # Implementation to read JSON configuration data
        ...         pass

    Note:
        Subclasses must implement the `read` method to provide format-specific
        configuration reading functionality.
    """

    @staticmethod
    def create(config_file_path: str) -> 'ConfigReader':
        
        path = config_file_path.lower()        
        if path.endswith('yaml') or path.endswith('yml'):
            return YAMLConfigReader()
        elif path.endswith('json'):
            return JsonConfigReader()
        else:
            raise ValueError(f"Unknown configuration format for configuration file: {path}")


    def read(self, config_file_path: str) -> Dict[str, Any]:
        """
        Reads and parses configuration data from a file.

        Args:
            config_file_path (str): The path to the configuration file.

        Returns:
            Dict[str, Any]: A dictionary containing the parsed configuration data.

        Raises:
            Any exceptions raised during file reading or parsing.
        """
        ...


class JsonConfigReader:
    
    def read(self, config_file_path: str) -> Dict[str, Any]:
        """
        Reads and parses configuration data from a JSON file.

        Args:
            config_file_path (str): The path to the JSON configuration file.

        Returns:
            Dict[str, Any]: A dictionary containing the parsed JSON configuration data.

        Raises:
            FileNotFoundError: If the specified file does not exist.
            json.JSONDecodeError: If there's an issue decoding the JSON data.
        """
        try:
            with open(config_file_path, 'r') as file:
                config_data = json.load(file)
                return config_data
        except FileNotFoundError:
            raise FileNotFoundError(f"JSON configuration file not found at {config_file_path}")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Error decoding JSON data in {config_file_path}", e.doc, e.pos)


class YAMLConfigReader:
    
    def read(self, config_file_path: str) -> Dict[str, Any]:
        """
        Reads and parses configuration data from a JSON file.

        Args:
            config_file_path (str): The path to the JSON configuration file.

        Returns:
            Dict[str, Any]: A dictionary containing the parsed JSON configuration data.

        Raises:
            FileNotFoundError: If the specified file does not exist.
            json.JSONDecodeError: If there's an issue decoding the JSON data.
        """
        try:
            with open(config_file_path, 'r') as file:
                config_data = yaml.safe_load(file)
                return config_data
        except FileNotFoundError:
            raise FileNotFoundError(f"YAML configuration file not found at {config_file_path}")
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error decoding YAML data in {config_file_path}")
