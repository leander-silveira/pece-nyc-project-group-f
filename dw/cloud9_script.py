import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from typing import List, Tuple

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

def read_and_normalize(spark: SparkSession, path: str, service_type: str) -> DataFrame:
    """
    Lê os arquivos Parquet de um tipo específico de táxi e normaliza os nomes e estruturas das colunas.

    :param spark: Sessão Spark.
    :param path: Caminho para os arquivos Parquet.
    :param service_type: Tipo do serviço (yellow, green, fhv, fhvhv).
    :return: DataFrame normalizado com colunas padronizadas e coluna service_type.
    """
    df = spark.read.option("basePath", path).parquet(f"{path}/year=*/month=*/*.parquet")
    df = df.withColumn("service_type", lit(service_type))

    # Renomeia colunas conforme padrão
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

    # Lista de colunas obrigatórias na camada DW
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
    # Adiciona colunas ausentes com valores nulos
    for col_name in required_cols:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None))

    return df

def load_all_trusted_data(spark: SparkSession, base_path: str) -> DataFrame:
    """
    Lê e unifica todos os dados da camada trusted para todos os tipos de táxi.

    :param spark: Sessão Spark.
    :param base_path: Caminho base da trusted no S3.
    :return: DataFrame unificado e normalizado com todos os dados.
    """
    sources = get_taxi_sources(base_path)
    dfs = []

    for path, service_type in sources:
        print(f"📥 Lendo dados de: {service_type.upper()} - {path}")
        df = read_and_normalize(spark, path, service_type)
        dfs.append(df)

    print("🔗 Unindo todos os DataFrames normalizados...")
    final_df = dfs[0]
    for df in dfs[1:]:
        final_df = final_df.unionByName(df, allowMissingColumns=True)

    return final_df

# Exemplo de execução principal
if __name__ == "__main__":
    spark = create_spark_session("NYC Taxi - Read Trusted")
    base_trusted_path = "s3a://mba-nyc-dataset/trusted"

    trusted_df = load_all_trusted_data(spark, base_trusted_path)

    print(f"✅ Dados carregados com sucesso: {trusted_df.count()} registros")
    trusted_df.printSchema()
    trusted_df.show(5)
