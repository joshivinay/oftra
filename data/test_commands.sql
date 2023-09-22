spark.sql("SELECT systems.column.ecord.description FROM delta.`/Users/vinay-templocal/temp/oftra/empower_14.delta`").show
spark.sql("SELECT systems.column.ecord.description FROM delta.`/Users/vinay-templocal/temp/oftra/empower_14.delta` VERSION AS OF 2").show
spark.sql("CREATE TABLE delta.`/Users/vinay-templocal/temp/oftra/empower_14_clone.delta` SHALLOW CLONE delta.`/Users/vinay-templocal/temp/oftra/empower_14.delta`").show

