from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    lit, to_date, date_format, hour, year, month, dayofweek, when, monotonically_increasing_id
)
from pyspark.sql.types import LongType, DoubleType, StringType
from typing import List, Tuple, Union

# Caminhos fixos utilizados
TRUSTED_PATH = "s3a://mba-nyc-dataset/trusted"
DW_PATH = "s3a://mba-nyc-dataset/dw"
RDS_JDBC_URL = "jdbc:mysql://nyc-dw-mysql.coseekllgrql.us-east-1.rds.amazonaws.com:3306/nyc_dw"
RDS_USER = "admin"
RDS_PASSWORD = "SuaSenhaForte123"

# Caminhos dos JARs locais no Cloud9
JARS_PATH = "/home/ec2-user/spark_jars/hadoop-aws-3.3.1.jar,/home/ec2-user/spark_jars/aws-java-sdk-bundle-1.11.901.jar,/home/ec2-user/spark_jars/mysql-connector-j-8.0.33.jar"

def create_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", JARS_PATH) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

def get_taxi_sources(base_path: str) -> List[Tuple[str, str]]:
    return [
        (f"{base_path}/yellowTaxi", "yellow")
    ]

def read_and_normalize(spark: SparkSession, path: str, service_type: str, years: List[int], months: Union[List[int], str]) -> DataFrame:
    if months == "*":
        parquet_paths = [f"{path}/year={year}/month=*/*.parquet" for year in years]
    else:
        parquet_paths = [f"{path}/year={year}/month={month}/*.parquet" for year in years for month in months]

    df = spark.read.option("basePath", path).parquet(*parquet_paths)
    df = df.withColumn("service_type", lit(service_type))

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
        "ehail_fee", "congestion_surcharge", "sales_tax", "bcf",
        "driver_pay", "store_and_fwd_flag", "shared_request_flag", "shared_match_flag",
        "access_a_ride_flag", "wav_request_flag", "wav_match_flag", "affiliated_base_number",
        "originating_base_number", "has_problem", "problem_description", "year", "month",
        "airport_fee"
    ]

    for col in required_cols:
        if col not in df.columns:
            dtype = DoubleType() if col in type_map or col.endswith("_amount") or col.endswith("_fee") else StringType()
            df = df.withColumn(col, lit(None).cast(dtype))

    return df

def load_all_trusted_data(spark: SparkSession, base_path: str, years: List[int], months: Union[List[int], str]) -> DataFrame:
    dfs = []
    for path, service_type in get_taxi_sources(base_path):
        print(f"📥 Lendo dados de: {service_type.upper()} - anos={years}, meses={months}")
        try:
            df = read_and_normalize(spark, path, service_type, years, months)
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Erro ao ler {service_type}: {e}")

    if not dfs:
        raise ValueError("❌ Nenhum dado foi carregado.")

    return dfs[0] if len(dfs) == 1 else dfs[0].unionByName(*dfs[1:], allowMissingColumns=True)

def write_parquet(df: DataFrame, table_name: str):
    output_path = f"{DW_PATH}/{table_name}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"💾 Tabela {table_name} salva no S3: {output_path}")

def write_to_rds(df: DataFrame, table_name: str):
    print(f"📤 Exportando para RDS: {table_name}")
    df.write.jdbc(
        url=RDS_JDBC_URL,
        table=table_name,
        mode="overwrite",
        properties={
            "user": RDS_USER,
            "password": RDS_PASSWORD,
            "driver": "com.mysql.cj.jdbc.Driver"
        }
    )

def save_table(df: DataFrame, table_name: str):
    write_parquet(df, table_name)
    write_to_rds(df, table_name)

def main():
    spark = create_spark_session("NYC Taxi - DW + RDS Export")
    anos = [2024]
    meses = [9, 10]  # Para teste apenas dois meses
    df = load_all_trusted_data(spark, TRUSTED_PATH, anos, meses)
    save_table(df, "fact_taxi_trip")
    print("✅ Processo concluído com sucesso!")

if __name__ == "__main__":
    main()
