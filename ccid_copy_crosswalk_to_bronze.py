# Databricks notebook source
# MAGIC  %run /Workspace/Shared/util/storage_account_access 

# COMMAND ----------

df = spark.sql(f""" SELECT * from silver_alwayson.combined_crosswalk """)
path = bronze_root + "bronze_schema/consumer_canvas/fusion/combined_crosswalk_corn"
table_name = "bronze_alwayson.combined_crosswalk_corn"

df.write.format("delta")\
    .option("compression", "snappy")\
    .option("path", path)\
    .saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql select * from bronze_alwayson.combined_crosswalk_corn
