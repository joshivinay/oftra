spark.sql("SELECT systems.column.ecord.description FROM delta.`/Users/vinay-templocal/temp/oftra/empower_14.delta`").show
spark.sql("SELECT systems.column.ecord.description FROM delta.`/Users/vinay-templocal/temp/oftra/empower_14.delta` VERSION AS OF 2").show
spark.sql("CREATE TABLE delta.`/Users/vinay-templocal/temp/oftra/empower_14_clone.delta` SHALLOW CLONE delta.`/Users/vinay-templocal/temp/oftra/empower_14.delta`").show


ASM -> Add metadata
Directly query 
API access
File access
Neural network
Mapping from IDS to ASM
Do away with Elastic


import org.zalando.spark.jsonschema.SchemaConverter
val schema1401Str = scala.io.Source.fromFile("/Users/joshivinay/Downloads/empower/v14.0.1/schema_14.0.1.json").mkString
val schema1401 = SchemaConverter.convertContent(schema1401Str)
val v14Data = spark.read.schema(schema1401).option("multiLine","true").json("/Users/joshivinay/Downloads/empower/v14.0.1/0.json")
v14Data.write.format("delta").save("/Users/joshivinay/Downloads/empower/v14Data.delta")

/** Drop column flat schema **/
val schema1401_delete_idstype_str = scala.io.Source.fromFile("/Users/joshivinay/Downloads/empower/v14.0.1/schema_drop_column_idsType.json").mkString
val schema1401_delete_idstype = SchemaConverter.convertContent(schema1401_delete_idstype_str)
val schema1401_delete_idstype_Data = spark.read.schema(schema1401_delete_idstype).option("multiLine","true").json("/Users/joshivinay/Downloads/empower/v14.0.1/0_drop_column_idsType.json")
schema1401_delete_idstype_Data.write.format("delta").option("mergeSchema","true").mode("append").save("/Users/joshivinay/Downloads/empower/v14Data.delta")
val xxx = spark.read.format("delta").load("/Users/joshivinay/Downloads/empower/v14Data.delta")

/** Drop column inside struct **/
val schema1401_delete_project_accesstype_str = scala.io.Source.fromFile("/Users/joshivinay/Downloads/empower/v14.0.1/schema_drop_column_inside_struct_project_access_type.json").mkString
val schema1401_delete_project_accesstype = SchemaConverter.convertContent(schema1401_delete_project_accesstype_str)
val schema1401_delete_project_accesstype_Data = spark.read.schema(schema1401_delete_project_accesstype).option("multiLine","true").json("/Users/joshivinay/Downloads/empower/v14.0.1/0_drop_column_project_access_type.json")
schema1401_delete_project_accesstype_Data.write.format("delta").option("mergeSchema","true").mode("append").save("/Users/joshivinay/Downloads/empower/v14Data.delta")
val xxx = spark.read.format("delta").load("/Users/joshivinay/Downloads/empower/v14Data.delta")

/** Drop column inside struct inside array **/
val schema1401_delete_runs_injection_transfer_time_str = scala.io.Source.fromFile("/Users/joshivinay/Downloads/empower/v14.0.1/schema_drop_column_struct_in_array_runs.json").mkString
val schema1401_delete_runs_injection_transfer_time = SchemaConverter.convertContent(schema1401_delete_runs_injection_transfer_time_str)
val schema1401_delete_runs_injection_transfertime_Data = spark.read.schema(schema1401_delete_runs_injection_transfer_time).option("multiLine","true").json("/Users/joshivinay/Downloads/empower/v14.0.1/0_drop_column_project_access_type.json")
schema1401_delete_runs_injection_transfertime_Data.write.format("delta").option("mergeSchema","true").mode("append").save("/Users/joshivinay/Downloads/empower/v14Data.delta")
val xxx = spark.read.format("delta").load("/Users/joshivinay/Downloads/empower/v14Data.delta")
xxx.select("runs.injection.transfer_time").show

/** Upcast column float to number **/


/** Downcast a column number ot float **/


/** Rename column in flat schema **/

