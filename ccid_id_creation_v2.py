# Databricks notebook source
# DBTITLE 1,Init
# MAGIC %run /Workspace/Shared/util/storage_account_access 

# COMMAND ----------

# DBTITLE 1,Run
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import uuid
import datetime
from delta.tables import *
import json

spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.selfDestruct.enabled", "true")

now = datetime.datetime.now()
date_str = now.strftime("%Y/%m/%d")
crosswalk_table_name = "bronze_alwayson.combined_crosswalk_corn"
tu_aiq_mri_table_name = "silver_alwayson.tu_aiq_mri_crosswalk"

tu_aiq_mri_crosswalk_table_name = "bronze_alwayson.combined_crosswalk_corn_tu_aiq_mri"
tu_aiq_mri_crosswalk_table_path = (
    bronze_root
    + "bronze_schema/consumer_canvas/fusion/combined_crosswalk_corn_tu_aiq_mri"
)

query = f"""
select
  a.AIQ_HHID,
  a.AIQ_INDID,
  b.*
from
  {tu_aiq_mri_table_name} a
  left join {crosswalk_table_name} b on a.TU_HHID = b.TU_HHID
  and a.TU_INDID = b.TU_INDID
"""

tu_aiq_mri_crosswalk_df = spark.sql(query)
display(tu_aiq_mri_crosswalk_df)

tu_aiq_mri_crosswalk_df.write.format("delta").option("compression", "snappy").option(
    "path", tu_aiq_mri_crosswalk_table_path
).saveAsTable(tu_aiq_mri_crosswalk_table_name)
