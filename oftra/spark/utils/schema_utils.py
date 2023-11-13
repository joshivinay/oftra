from pyspark.sql.types import StructType, DataType

class SchemaUtils(object):
    """
    Utility functions for schema operations
    """

    
    @staticmethod
    def get_schema_from_ddl(ddl_schema_path):
      with open(ddl_schema_path) as f:
        schema_as_str = f.read()     
        schema_as_struct_type = DataType.fromDDL(schema_as_str)
        return schema_as_struct_type
                

    
    @staticmethod
    def get_schema_from_json(json_schema_path):
        with open(json_schema_path) as f:
          schema_as_str = f.read()     
          schema_as_struct_type = StructType.fromJson(schema_as_str)
          return schema_as_struct_type        

    
    