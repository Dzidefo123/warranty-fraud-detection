# Databricks notebook source
from pyspark.sql.functions import col, isnan, count, when

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, count, when

def check_nulls_and_nans(df: DataFrame, df_name: str = "DataFrame"):
    """
    Display count of NULLs and NaNs for all columns in the given Spark DataFrame.

    Args:
        df (DataFrame): Spark DataFrame to analyze.
        df_name (str): Optional name for the DataFrame (for display context).
    """
    print(f"🔍 Checking nulls and NaNs in {df_name}...")

    # Create a dictionary of column types
    type_dict = dict(df.dtypes)

    # Build the expressions dynamically
    null_nan_exprs = []
    for c in df.columns:
        dtype = type_dict[c]
        # Only check isnan() for numeric columns
        if dtype in ['float', 'double']:
            expr = count(when(col(c).isNull() | isnan(col(c)), c)).alias(c)
        else:
            expr = count(when(col(c).isNull(), c)).alias(c)
        null_nan_exprs.append(expr)

    # Execute
    result = df.select(null_nan_exprs)
    result.show(truncate=False)
    return result


# COMMAND ----------

query = f"""
select * from warranty_fraud_db.d_vehicle_base"""
df_vehicle_base = spark.sql(query)

# COMMAND ----------

query = f"""
select * from warranty_fraud_db.dealer_info"""
df_dealer_info = spark.sql(query)

# COMMAND ----------

query = f"""
select * from warranty_fraud_db.f_claim_repair_header"""
df_claim_repair_header = spark.sql(query)

# COMMAND ----------

query = f"""
select * from warranty_fraud_db.f_claim_job"""
df_claim_job = spark.sql(query)

# COMMAND ----------

# Vehicle Base
check_nulls_and_nans(df_vehicle_base, "D_VEHICLE_BASE")
check_nulls_and_nans(df_dealer_info, "dealer_info")
check_nulls_and_nans(df_claim_repair_header, "F_CLAIM_REPAIR_HEADER")
check_nulls_and_nans(df_claim_job, "F_CLAIM_JOB")


# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def check_duplicates(df: DataFrame, id_cols: list, df_name: str = "DataFrame"):
    """
    Checks for duplicate records in the given Spark DataFrame based on one or more ID columns.

    Args:
        df (DataFrame): Spark DataFrame to analyze.
        id_cols (list): List of column names to check duplicates for.
        df_name (str): Optional name for display context.
    """
    print(f"🔍 Checking duplicates in {df_name} based on {id_cols}...")

    # Validate
    if not id_cols:
        print("⚠️ Please provide at least one ID column.")
        return
    
    # Group by ID columns and count occurrences
    dup_df = (
        df.groupBy(id_cols)
          .count()
          .filter(col("count") > 1)
    )

    # Display results
    if dup_df.count() == 0:
        print(f"✅ No duplicates found in {df_name}.")
    else:
        print(f"⚠️ Found {dup_df.count()} duplicate records in {df_name}:")
        dup_df.show(truncate=False)
    
    return dup_df


# COMMAND ----------

# Check duplicates in vehicle table
check_duplicates(df_vehicle_base, ["DEALER_ID"], "D_VEHICLE_BASE")
check_duplicates(df_dealer_info, ["DEALER_ID"], "dealer_info")
check_duplicates(df_claim_repair_header, ["CLAIM_ID"], "F_CLAIM_REPAIR_HEADER")
check_duplicates(df_claim_job, ["CLAIM_ID", "JOB_CODE"], "F_CLAIM_JOB")

# COMMAND ----------

## out-of-range years (future model years beyond 2025)
df_vehicle_base.filter(col('MODEL_YEAR') > 2025
).show()

# COMMAND ----------

## negative or unrealistic mileage values
df_vehicle_base.filter(col('MILEAGE') < 0).show()
