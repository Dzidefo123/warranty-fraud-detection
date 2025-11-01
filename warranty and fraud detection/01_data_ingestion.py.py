# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
import datetime
import builtins

spark = SparkSession.builder \
    .appName("Warranty Fraud Synthetic Data Generator") \
    .enableHiveSupport() \
    .getOrCreate()

# COMMAND ----------

# -----------------------------
# PARAMETERS
# -----------------------------
NUM_VEHICLES = 1000
NUM_DEALERS = 50
NUM_CLAIMS = 5000
VEHICLE_MODELS = ["FH16", "FMX", "VNR", "VNXL", "FM", "FE"]
REGIONS = ["Brazil", "Sweden", "USA", "Canada", "France", "India"]
CLAIM_TYPES = ["Warranty", "Recall", "Goodwill"]
CLAIM_STATUS = ["Approved", "Rejected", "Pending"]

# -----------------------------
# 1. DEALER INFO
# -----------------------------
dealer_data = []
for i in range(1, NUM_DEALERS + 1):
    dealer_id = f"D{str(i).zfill(4)}"
    region = random.choice(REGIONS)
    total_claims = random.randint(50, 1000)
    fraud_history = builtins.round(random.uniform(0.0, 0.25), 2)  # Use builtins.round
    dealer_data.append((dealer_id, region, total_claims, fraud_history))

dealer_schema = StructType([
    StructField("DEALER_ID", StringType(), False),
    StructField("REGION", StringType(), True),
    StructField("TOTAL_CLAIMS", IntegerType(), True),
    StructField("FRAUD_HISTORY", DoubleType(), True)
])

df_dealer = spark.createDataFrame(dealer_data, schema=dealer_schema)

# -----------------------------
# SHOW & SAVE
# -----------------------------
#display(df_dealer)
df_dealer.write.format("delta").mode("overwrite").saveAsTable("warranty_fraud_db.dealer_info")

# COMMAND ----------

# -----------------------------
# 2. VEHICLE BASE
# -----------------------------
vehicle_data = [
    (
        f"V{str(i).zfill(6)}",
        random.randint(2015, 2024),
        random.choice(VEHICLE_MODELS),
        (datetime.date(2025, 1, 1) - datetime.timedelta(days=random.randint(0, 365*5))),
        builtins.round(random.uniform(10000, 400000), 2)
    )
    for i in range(1, NUM_VEHICLES + 1)
]

vehicle_schema = StructType([
    StructField("VEHICLE_ID", StringType(), False),
    StructField("MODEL_YEAR", IntegerType(), True),
    StructField("VEHICLE_MODEL", StringType(), True),
    StructField("WARRANTY_END_DATE", DateType(), True),
    StructField("MILEAGE", DoubleType(), True)
])

df_vehicle = spark.createDataFrame(vehicle_data, schema=vehicle_schema)
df_vehicle.write.format("delta").mode("overwrite").saveAsTable("warranty_fraud_db.d_vehicle_base")


# COMMAND ----------

# -----------------------------
# 3. CLAIM REPAIR HEADER
# -----------------------------
def random_date(start_year=2020, end_year=2025):
    start_date = datetime.date(start_year, 1, 1)
    end_date = datetime.date(end_year, 10, 1)
    delta = end_date - start_date
    return (start_date + datetime.timedelta(days=random.randint(0, delta.days)))

claim_data = []
for i in range(1, NUM_CLAIMS + 1):
    claim_id = f"C{str(i).zfill(6)}"
    claim_date = random_date()
    dealer_id = random.choice(dealer_data)[0]
    vehicle_id = random.choice(vehicle_data)[0]
    claim_amount = builtins.round(random.uniform(500, 15000), 2)
    claim_status = random.choice(CLAIM_STATUS)
    claim_type = random.choice(CLAIM_TYPES)
    
    # Fraud probability influenced by dealer and claim type
    fraud_base = 0.05 + next(d[3] for d in dealer_data if d[0] == dealer_id)
    if claim_type == "Goodwill": fraud_base += 0.05
    is_fraud = random.random() < fraud_base
    
    claim_data.append((
        claim_id, claim_date, dealer_id, vehicle_id,
        claim_amount, claim_status, claim_type, is_fraud
    ))

claim_schema = StructType([
    StructField("CLAIM_ID", StringType(), False),
    StructField("CLAIM_DATE", DateType(), True),
    StructField("DEALER_ID", StringType(), True),
    StructField("VEHICLE_ID", StringType(), True),
    StructField("CLAIM_AMOUNT", DoubleType(), True),
    StructField("CLAIM_STATUS", StringType(), True),
    StructField("CLAIM_TYPE", StringType(), True),
    StructField("IS_FRAUD", BooleanType(), True)
])

df_claim_header = spark.createDataFrame(claim_data, schema=claim_schema)
df_claim_header.write.format("delta").mode("overwrite").saveAsTable("warranty_fraud_db.f_claim_repair_header")

# COMMAND ----------

# -----------------------------
# 4. CLAIM JOB
# -----------------------------
job_data = []
for row in df_claim_header.select("CLAIM_ID").collect():
    claim_id = row["CLAIM_ID"]
    num_jobs = random.randint(1, 3)
    for j in range(num_jobs):
        job_data.append((
            claim_id,
            f"JOB_{random.randint(100,999)}",
            builtins.round(random.uniform(1, 10), 2),
            builtins.round(random.uniform(100, 2000), 2)
        ))

job_schema = StructType([
    StructField("CLAIM_ID", StringType(), False),
    StructField("JOB_CODE", StringType(), True),
    StructField("JOB_HOURS", DoubleType(), True),
    StructField("JOB_COST", DoubleType(), True)
])

df_claim_job = spark.createDataFrame(job_data, schema=job_schema)
df_claim_job.write.format("delta").mode("overwrite").saveAsTable("warranty_fraud_db.f_claim_job")
