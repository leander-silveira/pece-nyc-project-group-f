from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    lit, to_date, date_format, hour, year, month, dayofweek, when, monotonically_increasing_id
)
from pyspark.sql.types import LongType, DoubleType
from typing import List, Tuple, Union

# Caminho da camada trusted e destino da camada dw
TRUSTED_PATH = "s3a://mba-nyc-dataset/trusted"
DW_PATH = "s3a://mba-nyc-dataset/dw"

def create_spark_session(app_name: str) -> SparkSession:
    """
    Cria e configura uma SparkSession para leitura de dados no S3.
    """
    jars_path = "/home/ec2-user/spark_jars/hadoop-aws-3.3.1.jar,/home/ec2-user/spark_jars/aws-java-sdk-bundle-1.11.901.jar"
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", jars_path) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()
    return spark

def get_taxi_sources(base_path: str) -> List[Tuple[str, str]]:
    """
    Define os caminhos e os tipos de serviço a serem lidos.

    :param base_path: Caminho base do bucket S3 onde estão os dados trusted.
    :return: Lista de tuplas com (caminho completo, tipo de serviço).
    """
    return [
        (f"{base_path}/yellowTaxi", "yellow"),
        (f"{base_path}/greenTaxi", "green"),
        (f"{base_path}/forHireVehicle", "fhv"),
        (f"{base_path}/highVolumeForHire", "fhvhv")
    ]


def read_and_normalize(
    spark: SparkSession,
    path: str,
    service_type: str,
    years: List[int],
    months: Union[List[int], str] = "*"
) -> DataFrame:
    """
    Lê os arquivos Parquet de um tipo específico de táxi e normaliza os nomes e estruturas das colunas.

    :param spark: Sessão Spark.
    :param path: Caminho para os arquivos Parquet.
    :param service_type: Tipo do serviço (yellow, green, fhv, fhvhv).
    :return: DataFrame normalizado com colunas padronizadas e coluna service_type.
    """
    if months == "*":
        parquet_paths = [f"{path}/year={year}/month=*/*.parquet" for year in years]
    else:
        parquet_paths = [f"{path}/year={year}/month={month}/*.parquet" for year in years for month in months]

    df = spark.read.option("basePath", path).parquet(*parquet_paths)
    df = df.withColumn("service_type", lit(service_type))

    # Renomeia colunas
    rename_map = {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "lpep_pickup_datetime": "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pickup_location_id",
        "DOlocationID": "dropoff_location_id",
        "RatecodeID": "ratecode_id",
        "VendorID": "vendor_id",
        "Affiliated_base_number": "affiliated_base_number",
        "originating_base_num": "originating_base_number"
    }
    for old_col, new_col in rename_map.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)

    from pyspark.sql.types import LongType, DoubleType
    type_map = {
        "vendor_id": LongType(),
        "ratecode_id": DoubleType(),
        "payment_type": DoubleType(),
        "trip_type": DoubleType(),
        "pickup_location_id": DoubleType(),
        "dropoff_location_id": DoubleType()
    }
    for col_name, data_type in type_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, df[col_name].cast(data_type))

    required_cols = [
        "pickup_datetime", "dropoff_datetime", "vendor_id", "ratecode_id",
        "payment_type", "trip_type", "pickup_location_id", "dropoff_location_id",
        "passenger_count", "trip_distance", "fare_amount", "tip_amount",
        "tolls_amount", "total_amount", "extra", "mta_tax", "improvement_surcharge",
        "ehail_fee", "congestion_surcharge", "airport_fee", "sales_tax", "bcf",
        "driver_pay", "store_and_fwd_flag", "shared_request_flag", "shared_match_flag",
        "access_a_ride_flag", "wav_request_flag", "wav_match_flag", "affiliated_base_number",
        "originating_base_number", "has_problem", "problem_description", "year", "month"
    ]
    for col_name in required_cols:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None))

    return df

def load_all_trusted_data(spark: SparkSession, base_path: str, years: List[int], months: Union[List[int], str]) -> DataFrame:
    """
    Lê e unifica todos os dados da camada trusted para todos os tipos de táxi.

    :param spark: Sessão Spark.
    :param base_path: Caminho base da trusted no S3.
    :return: DataFrame unificado e normalizado com todos os dados.
    """
    sources = get_taxi_sources(base_path)
    dfs = []

    for path, service_type in sources:
        print(f"📥 Lendo dados de: {service_type.upper()} - {path} - anos={years}, meses={months}")
        try:
            df = read_and_normalize(spark, path, service_type, years, months)
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Erro ao ler {service_type}: {e}")

    if not dfs:
        raise ValueError("❌ Nenhum dado foi carregado.")

    print("🔗 Unindo todos os DataFrames normalizados...")
    final_df = dfs[0]
    for df in dfs[1:]:
        final_df = final_df.unionByName(df, allowMissingColumns=True)

    return final_df

def write_parquet(df: DataFrame, table_name: str):
    output_path = f"{DW_PATH}/{table_name}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"✅ Tabela {table_name} salva em: {output_path}")

def build_and_save_fact_table(df: DataFrame):
    df = df.withColumn("pickup_date", to_date("pickup_datetime")) \
           .withColumn("dropoff_date", to_date("dropoff_datetime")) \
           .withColumn("trip_duration_minutes", (df["dropoff_datetime"].cast("long") - df["pickup_datetime"].cast("long")) / 60) \
           .withColumn("weekday", date_format("pickup_datetime", "EEEE")) \
           .withColumn("hour", hour("pickup_datetime")) \
           .withColumn("fk_time", date_format("dropoff_datetime", "ddMMyyyy").cast("int")) \
           .withColumn("sk_trip", monotonically_increasing_id())

    fact = df.select(
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
    write_parquet(fact, "fact_taxi_trip")

def build_and_save_dim_time(df: DataFrame):
    df = df.withColumn("dropoff_date", to_date("dropoff_datetime"))
    dim_time = df.select("dropoff_date") \
        .withColumn("sk_time", date_format("dropoff_date", "ddMMyyyy").cast("int")) \
        .withColumn("date", date_format("dropoff_date", "yyyyMMdd").cast("int")) \
        .withColumn("weekday", date_format("dropoff_date", "EEEE")) \
        .withColumn("month", month("dropoff_date")) \
        .withColumn("year", year("dropoff_date")) \
        .withColumn("season", when((month("dropoff_date") <= 2) | (month("dropoff_date") == 12), "Winter")
                              .when((month("dropoff_date") <= 5), "Spring")
                              .when((month("dropoff_date") <= 8), "Summer")
                              .otherwise("Fall")) \
        .withColumn("is_weekend", dayofweek("dropoff_date").isin([1, 7])) \
        .withColumn("is_holiday", lit(False)) \
        .dropDuplicates(["sk_time"])
    write_parquet(dim_time, "dim_time")

def build_and_save_dim_service_type(spark: SparkSession):
    dim = spark.createDataFrame([
        ("yellow", "Yellow Taxi", "Taxi", "TLC"),
        ("green", "Green Taxi", "Taxi", "TLC"),
        ("fhv", "For-Hire Vehicle", "For-Hire", "Empresas Privadas"),
        ("fhvhv", "High-Volume For-Hire Vehicle", "For-Hire", "Empresas Privadas"),
    ], ["pk_service_type", "description", "category", "regulation_body"])
    write_parquet(dim, "dim_service_type")

def build_and_save_dim_location(spark: SparkSession):
    zone_lookup = spark.read.option("header", True).csv("s3a://mba-nyc-dataset/reference/taxi_zone_lookup.csv")
    dim = zone_lookup.selectExpr("LocationID as pk_location", "Borough as borough", "Zone as zone", "service_zone").dropDuplicates()
    write_parquet(dim, "dim_location")

def build_and_save_dim_payment_type(spark: SparkSession):
    dim = spark.createDataFrame([
        (1, "Credit Card"),
        (2, "Cash"),
        (3, "No Charge"),
        (4, "Dispute"),
        (5, "Unknown"),
        (6, "Voided Trip")
    ], ["pk_payment_type", "description"])
    write_parquet(dim, "dim_payment_type")

def build_and_save_dim_vendor(df: DataFrame):
    dim = df.select("vendor_id").where(df["vendor_id"].isNotNull()).distinct() \
            .withColumn("description", lit("Vendor desconhecido"))
    write_parquet(dim, "dim_vendor")

def build_and_save_dim_ratecode(spark: SparkSession):
    dim = spark.createDataFrame([
        (1, "Standard rate"),
        (2, "JFK"),
        (3, "Newark"),
        (4, "Nassau or Westchester"),
        (5, "Negotiated fare"),
        (6, "Group ride")
    ], ["ratecode_id", "description"])
    write_parquet(dim, "dim_ratecode")

if __name__ == "__main__":
    spark = create_spark_session("NYC Taxi - DW")

    anos = [2024]
    meses = [10]

    trusted_df = load_all_trusted_data(spark, TRUSTED_PATH, anos, meses)

    build_and_save_fact_table(trusted_df)
    build_and_save_dim_time(trusted_df)
    build_and_save_dim_service_type(spark)
    build_and_save_dim_location(spark)
    build_and_save_dim_payment_type(spark)
    build_and_save_dim_vendor(trusted_df)
    build_and_save_dim_ratecode(spark)

    print("✅ Todas as tabelas foram salvas com sucesso na camada DW.")
