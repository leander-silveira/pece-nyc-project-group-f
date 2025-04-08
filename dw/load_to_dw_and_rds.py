from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    lit, to_date, date_format, hour, year, month, dayofweek, when, monotonically_increasing_id
)
from pyspark.sql.types import LongType, DoubleType
from typing import List, Tuple, Union

# Caminhos fixos utilizados
TRUSTED_PATH = "s3a://mba-nyc-dataset/trusted"
DW_PATH = "s3a://mba-nyc-dataset/dw"
RDS_JDBC_JAR = "s3://mba-nyc-dataset/emr/jars/mysql-connector-j-8.0.33.jar"
RDS_JDBC_URL = "jdbc:mysql://nyc-dw-mysql.coseekllgrql.us-east-1.rds.amazonaws.com:3306/nyc_dw"
RDS_USER = "admin"
RDS_PASSWORD = "SuaSenhaForte123"

def create_spark_session(app_name: str) -> SparkSession:
    """
    Cria uma SparkSession com configurações específicas para:
    - Leitura de dados no S3 via S3A;
    - Escrita em banco de dados MySQL via JDBC.

    :param app_name: Nome da aplicação Spark.
    :return: Objeto SparkSession configurado.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", RDS_JDBC_JAR) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

def get_taxi_sources(base_path: str) -> List[Tuple[str, str]]:
    """
    Retorna os caminhos e tipos de serviço de táxi disponíveis na camada trusted.

    :param base_path: Caminho base onde estão os dados trusted no S3.
    :return: Lista de tuplas com caminho e tipo de serviço.
    """
    return [
        (f"{base_path}/yellowTaxi", "yellow"),
        (f"{base_path}/greenTaxi", "green"),
        (f"{base_path}/forHireVehicle", "fhv"),
        (f"{base_path}/highVolumeForHire", "fhvhv")
    ]

def read_and_normalize(spark: SparkSession, path: str, service_type: str, years: List[int], months: Union[List[int], str]) -> DataFrame:
    """
    Lê arquivos Parquet de um tipo de táxi, renomeia colunas, normaliza tipos e adiciona campos obrigatórios.

    :param spark: Sessão Spark ativa.
    :param path: Caminho no S3 para os dados.
    :param service_type: Tipo de serviço (ex: yellow, green).
    :param years: Lista de anos para filtro.
    :param months: Lista de meses ou '*' para todos.
    :return: DataFrame padronizado.
    """
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
        "ehail_fee", "congestion_surcharge", "airport_fee", "sales_tax", "bcf",
        "driver_pay", "store_and_fwd_flag", "shared_request_flag", "shared_match_flag",
        "access_a_ride_flag", "wav_request_flag", "wav_match_flag", "affiliated_base_number",
        "originating_base_number", "has_problem", "problem_description", "year", "month"
    ]
    for col in required_cols:
        if col not in df.columns:
            df = df.withColumn(col, lit(None))

    return df

def load_all_trusted_data(spark: SparkSession, base_path: str, years: List[int], months: Union[List[int], str]) -> DataFrame:
    """
    Carrega todos os dados das fontes trusted, normaliza e unifica em um único DataFrame.

    :param spark: Sessão Spark.
    :param base_path: Caminho base dos dados trusted no S3.
    :param years: Lista de anos a serem lidos.
    :param months: Lista de meses ou '*' para todos.
    :return: DataFrame consolidado com todos os dados.
    """
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
    """
    Escreve um DataFrame como tabela Parquet na camada DW (S3).

    :param df: DataFrame a ser salvo.
    :param table_name: Nome da tabela destino.
    """
    output_path = f"{DW_PATH}/{table_name}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"💾 Tabela {table_name} salva no S3: {output_path}")

def write_to_rds(df: DataFrame, table_name: str):
    """
    Exporta um DataFrame para o banco MySQL RDS usando JDBC.

    :param df: DataFrame a ser exportado.
    :param table_name: Nome da tabela destino no RDS.
    """
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
    """
    Salva uma tabela em dois destinos:
    - Como Parquet no S3 (camada DW);
    - Como tabela no MySQL RDS.

    :param df: DataFrame a ser salvo.
    :param table_name: Nome da tabela nos destinos.
    """
    write_parquet(df, table_name)
    write_to_rds(df, table_name)

def main():
    """
    Função principal que executa o pipeline:
    - Lê dados da camada trusted (anos e meses especificados);
    - Normaliza e unifica os dados;
    - Escreve os dados em S3 e no RDS.
    """
    spark = create_spark_session("NYC Taxi - DW + RDS Export")

    anos = [2022, 2023, 2024]  # obrigatório
    meses = "*"  # use '*' para todos os meses ou especifique uma lista como [3, 4]

    df = load_all_trusted_data(spark, TRUSTED_PATH, anos, meses)
    save_table(df, "fact_taxi_trip")
    print("✅ Processo concluído com sucesso!")

if __name__ == "__main__":
    main()
