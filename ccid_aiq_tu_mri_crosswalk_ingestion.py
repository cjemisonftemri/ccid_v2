# Databricks notebook source
# MAGIC  %run /Workspace/Shared/util/storage_account_access 

# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import uuid
import datetime
from delta.tables import *
import json

input_path = transunion_root + "Crosswalk/MRI_TU_AIQ_Crosswalk_August2024_08.05.2024/"
output_path = bronze_root + "bronze_schema/transunion/crosswalk/2004/08/05"
table_name = "bronze_alwayson.tu_aiq_mri_crosswalk"

spark.sql(f"drop table if exists {table_name}")
df = spark.read.parquet(input_path)
df.write\
    .format("delta")\
    .option("compression", "snappy")\
    .option("path", output_path)\
    .saveAsTable(table_name)


# COMMAND ----------

output_path = bronze_root + "silver_schema/transunion/crosswalk/2004/08/05"
table_name = "silver_alwayson.tu_aiq_mri_crosswalk"
df = spark.read.parquet(input_path)
 
@F.udf(returnType=StringType())
def split_aiq_id(s: str, x: int)-> str:
    tmp = None
    if s:
        l = s.split("_")
        if l:
            tmp = l[x]
    return tmp

df = (
    df.withColumnRenamed("MRI_Extern_TU_HHID", "TU_HHID")
    .withColumnRenamed("MRI_Extern_TU_INDID", "TU_INDID")
    .withColumn("AIQ_HHID", split_aiq_id(F.col("AIQI_INDHHID"), F.lit(0)))
    .withColumn("AIQ_INDID", split_aiq_id(F.col("AIQI_INDHHID"), F.lit(1)))
    .drop("AIQI_INDHHID")
)

df.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", True)\
    .save(output_path)

# COMMAND ----------

# MAGIC %sql select * from silver_alwayson.tu_aiq_mri_crosswalk
