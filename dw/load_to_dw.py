from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, year, month, dayofmonth, hour, dayofweek, when, concat_ws, to_date, date_format, monotonically_increasing_id
import calendar

# Inicializa a sessão Spark
spark = SparkSession.builder \
    .appName("NYC Taxi - DW") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

# Caminho da camada trusted e destino da camada dw
TRUSTED_PATH = "s3a://mba-nyc-dataset/trusted/"
DW_PATH = "s3a://mba-nyc-dataset/dw/"

# Leitura dos dados trusted (todos os tipos de taxi)
trusted_df = spark.read.option("basePath", TRUSTED_PATH).parquet(f"{TRUSTED_PATH}*/*/*.parquet")

# Adiciona colunas derivadas
df = trusted_df \
    .withColumn("pickup_date", to_date("pickup_datetime")) \
    .withColumn("dropoff_date", to_date("dropoff_datetime")) \
    .withColumn("trip_duration_minutes", (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60) \
    .withColumn("weekday", date_format(col("pickup_datetime"), "EEEE")) \
    .withColumn("hour", hour("pickup_datetime")) \
    .withColumn("fk_time", date_format("dropoff_datetime", "ddMMyyyy").cast("int")) \
    .withColumn("sk_trip", monotonically_increasing_id())

# Tabela Fato
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

# Tabelas Dimensões
# dim_time
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

# dim_service_type
dim_service_type = spark.createDataFrame([
    ("yellow", "Yellow Taxi", "Taxi", "TLC"),
    ("green", "Green Taxi", "Taxi", "TLC"),
    ("fhv", "For-Hire Vehicle", "For-Hire", "Empresas Privadas"),
    ("fhvhv", "High-Volume For-Hire Vehicle", "For-Hire", "Empresas Privadas"),
], ["pk_service_type", "description", "category", "regulation_body"])

dim_service_type.write.mode("overwrite").parquet(f"{DW_PATH}dim_service_type")

# dim_location (assumindo que o zone_lookup foi carregado anteriormente)
zone_lookup = spark.read.option("header", True).csv("s3a://mba-nyc-dataset/reference/zone_lookup.csv")
dim_location = zone_lookup.selectExpr("LocationID as pk_location", "Borough as borough", "Zone as zone", "service_zone").dropDuplicates()
dim_location.write.mode("overwrite").parquet(f"{DW_PATH}dim_location")

# dim_payment_type (valores fixos TLC)
dim_payment_type = spark.createDataFrame([
    (1, "Credit Card"),
    (2, "Cash"),
    (3, "No Charge"),
    (4, "Dispute"),
    (5, "Unknown"),
    (6, "Voided Trip")
], ["pk_payment_type", "description"])

dim_payment_type.write.mode("overwrite").parquet(f"{DW_PATH}dim_payment_type")

# dim_ratecode
dim_ratecode = spark.createDataFrame([
    (1, "Standard rate"),
    (2, "JFK"),
    (3, "Newark"),
    (4, "Nassau or Westchester"),
    (5, "Negotiated fare"),
    (6, "Group ride")
], ["ratecode_id", "description"])

dim_ratecode.write.mode("overwrite").parquet(f"{DW_PATH}dim_ratecode")

# dim_vendor (distintos dos trusted)
dim_vendor = df.select("vendor_id").where(col("vendor_id").isNotNull()).distinct().withColumnRenamed("vendor_id", "vendor_id") \
    .withColumn("description", lit("Vendor desconhecido"))
dim_vendor.write.mode("overwrite").parquet(f"{DW_PATH}dim_vendor")

# dim_trip_type (valores padrão)
dim_trip_type = spark.createDataFrame([
    (1, "Street-hail"),
    (2, "Dispatch")
], ["trip_type_id", "description"])

dim_trip_type.write.mode("overwrite").parquet(f"{DW_PATH}dim_trip_type")

print("✅ Dados da camada DW gerados com sucesso.")
