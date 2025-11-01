# Databricks notebook source
from pyspark.sql.functions import col, isnan, count, when

# COMMAND ----------

query = f"""
select * from warranty_fraud_db.d_vehicle_base"""
df_vehicle_base = spark.sql(query)

# COMMAND ----------

df_vehicle_base.select([
    count(
        when(
            col(c).isNull() | 
            (isnan(col(c)) if dict(df_vehicle_base.dtypes)[c] in ['float', 'double'] else False),
            c
        )
    ).alias(c)
    for c in df_vehicle_base.columns
]).show()

# COMMAND ----------

## check for duplicate ids
df_vehicle_base.groupBy('VEHICLE_ID').count().filter(col('count') > 1).show()

# COMMAND ----------

## out-of-range years (future model years beyond 2025)
df_vehicle_base.filter(col('MODEL_YEAR') > 2025
).show()

# COMMAND ----------

## negative or unrealistic mileage values
df_vehicle_base.filter(col('MILEAGE') < 0).show()
