import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, year, month, dayofmonth, hour, dayofweek, when, to_date, date_format, monotonically_increasing_id
import calendar

# Caminho para JARs no Cloud9
home_dir = os.environ["HOME"]
jars_path = f"{home_dir}/spark_jars/hadoop-aws-3.3.1.jar,{home_dir}/spark_jars/aws-java-sdk-bundle-1.11.901.jar"

# Inicializa Spark
spark = SparkSession.builder \
    .appName("NYC Taxi - DW") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.jars", jars_path) \
    .config("spark.driver.extraClassPath", jars_path) \
    .config("spark.executor.extraClassPath", jars_path) \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

TRUSTED_PATH = "s3a://mba-nyc-dataset/trusted/"
DW_PATH = "s3a://mba-nyc-dataset/dw/"

def read_and_normalize(path: str, service_type: str):
    df = spark.read.option("basePath", path).parquet(f"{path}/year=*/month=*/*.parquet")
    df = df.withColumn("service_type", lit(service_type))

    # Normalização por tipo
    if service_type == "yellow":
        df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
               .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    elif service_type == "green":
        df = df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
               .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

    # Renomeia colunas comuns para todos os tipos (se existirem)
    renames = {
        "VendorID": "vendor_id",
        "RatecodeID": "ratecode_id",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "payment_type": "payment_type",
        "Affiliated_base_number": "affiliated_base_number",
        "originating_base_num": "originating_base_number"
    }
    for old, new in renames.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    # Adiciona colunas ausentes para alinhamento
    required_cols = ["pickup_datetime", "dropoff_datetime", "passenger_count"]
    for col_name in required_cols:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast("string" if "flag" in col_name else "double"))

    return df

trusted_yellow = read_and_normalize(f"{TRUSTED_PATH}yellowTaxi", "yellow")
trusted_green = read_and_normalize(f"{TRUSTED_PATH}greenTaxi", "green")
trusted_fhv = read_and_normalize(f"{TRUSTED_PATH}forHireVehicle", "fhv")
trusted_fhvhv = read_and_normalize(f"{TRUSTED_PATH}highVolumeForHire", "fhvhv")

trusted_df = trusted_yellow.unionByName(trusted_green, allowMissingColumns=True) \
                           .unionByName(trusted_fhv, allowMissingColumns=True) \
                           .unionByName(trusted_fhvhv, allowMissingColumns=True)

df = trusted_df \
    .withColumn("pickup_date", to_date("pickup_datetime")) \
    .withColumn("dropoff_date", to_date("dropoff_datetime")) \
    .withColumn("trip_duration_minutes", (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60) \
    .withColumn("weekday", date_format(col("pickup_datetime"), "EEEE")) \
    .withColumn("hour", hour("pickup_datetime")) \
    .withColumn("fk_time", date_format("dropoff_datetime", "ddMMyyyy").cast("int")) \
    .withColumn("sk_trip", monotonically_increasing_id())

fact_taxi_trip = df.select(
    "sk_trip", "vendor_id", "ratecode_id", "trip_type", "payment_type",
    "fk_time", "service_type", "pickup_datetime", "dropoff_datetime",
    "trip_duration_minutes", "pickup_location_id", "dropoff_location_id",
    "passenger_count", "trip_distance", "fare_amount", "tip_amount",
    "tolls_amount", "total_amount", "extra", "mta_tax", "improvement_surcharge",
    "ehail_fee", "congestion_surcharge", "airport_fee", "sales_tax", "bcf",
    "driver_pay", "store_and_fwd_flag", "shared_request_flag", "shared_match_flag",
    "access_a_ride_flag", "wav_request_flag", "wav_match_flag", "affiliated_base_number",
    "originating_base_number", "has_problem", "problem_description", "year", "month",
    "weekday", "hour"
)

fact_taxi_trip.write.mode("overwrite").parquet(f"{DW_PATH}fact_taxi_trip")

def get_season(month):
    return (
        when((month == 12) | (month <= 2), "Winter")
        .when((month >= 3) & (month <= 5), "Spring")
        .when((month >= 6) & (month <= 8), "Summer")
        .when((month >= 9) & (month <= 11), "Fall")
    )

dim_time = df.select("dropoff_date") \
    .withColumn("sk_time", date_format("dropoff_date", "ddMMyyyy").cast("int")) \
    .withColumn("date", date_format("dropoff_date", "yyyyMMdd").cast("int")) \
    .withColumn("weekday", date_format("dropoff_date", "EEEE")) \
    .withColumn("month", month("dropoff_date")) \
    .withColumn("year", year("dropoff_date")) \
    .withColumn("season", get_season(month("dropoff_date"))) \
    .withColumn("is_weekend", dayofweek("dropoff_date").isin([1, 7])) \
    .withColumn("is_holiday", lit(False)) \
    .dropDuplicates(["sk_time"])

dim_time.write.mode("overwrite").parquet(f"{DW_PATH}dim_time")

dim_service_type = spark.createDataFrame([
    ("yellow", "Yellow Taxi", "Taxi", "TLC"),
    ("green", "Green Taxi", "Taxi", "TLC"),
    ("fhv", "For-Hire Vehicle", "For-Hire", "Empresas Privadas"),
    ("fhvhv", "High-Volume For-Hire Vehicle", "For-Hire", "Empresas Privadas"),
], ["pk_service_type", "description", "category", "regulation_body"])

dim_service_type.write.mode("overwrite").parquet(f"{DW_PATH}dim_service_type")

zone_lookup = spark.read.option("header", True).csv("s3a://mba-nyc-dataset/reference/zone_lookup.csv")
dim_location = zone_lookup.selectExpr("LocationID as pk_location", "Borough as borough", "Zone as zone", "service_zone").dropDuplicates()
dim_location.write.mode("overwrite").parquet(f"{DW_PATH}dim_location")

dim_payment_type = spark.createDataFrame([
    (1, "Credit Card"),
    (2, "Cash"),
    (3, "No Charge"),
    (4, "Dispute"),
    (5, "Unknown"),
    (6, "Voided Trip")
], ["pk_payment_type", "description"])

dim_payment_type.write.mode("overwrite").parquet(f"{DW_PATH}dim_payment_type")

dim_ratecode = spark.createDataFrame([
    (1, "Standard rate"),
    (2, "JFK"),
    (3, "Newark"),
    (4, "Nassau or Westchester"),
    (5, "Negotiated fare"),
    (6, "Group ride")
], ["ratecode_id", "description"])

dim_ratecode.write.mode("overwrite").parquet(f"{DW_PATH}dim_ratecode")

dim_vendor = df.select("vendor_id").where(col("vendor_id").isNotNull()).distinct() \
    .withColumn("description", lit("Vendor desconhecido"))
dim_vendor.write.mode("overwrite").parquet(f"{DW_PATH}dim_vendor")

dim_trip_type = spark.createDataFrame([
    (1, "Street-hail"),
    (2, "Dispatch")
], ["trip_type_id", "description"])

dim_trip_type.write.mode("overwrite").parquet(f"{DW_PATH}dim_trip_type")

print("✅ Dados da camada DW gerados com sucesso.")
