import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat_ws

# Caminho para JARs no Cloud9
# home_dir = os.environ["HOME"]
# jars_path = f"{home_dir}/spark_jars/hadoop-aws-3.3.1.jar,{home_dir}/spark_jars/aws-java-sdk-bundle-1.11.901.jar"

# Inicializa Spark
spark = SparkSession.builder \
    .appName("NYC Taxi Trusted Transform") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Mapeamento dos tipos de táxi
TAXI_TYPES = {
    'fhv_tripdata': 'forHireVehicle',
    'green_tripdata': 'greenTaxi',
    'yellow_tripdata': 'yellowTaxi',
    'fhvhv_tripdata': 'highVolumeForHire',
}

# Substituir NULO por 0 nos seguintes datasets:
# FHV = coluna SR_Flag

def apply_quality_dimensions(df, taxi_type):
  print(f"Aplicando regras de limpeza para tipo: {taxi_type}")
  df = df.withColumn("has_problem", lit(False))
  df = df.withColumn("problem_description", lit(""))

  # Dimensões de qualidade para dataset de taxis verdes e amarelos
  if taxi_type in ['yellowTaxi', 'greenTaxi']:
    pickup_col = 'tpep_pickup_datetime' if taxi_type == 'yellowTaxi' else 'lpep_pickup_datetime'
    dropoff_col = 'tpep_dropoff_datetime' if taxi_type == 'yellowTaxi' else 'lpep_dropoff_datetime'

    # Verificação de valores inválidos
    df = df.withColumn("passenger_count",when(col("passenger_count")<=0, lit(1)).otherwise(col("passenger_count")))
    df = df.filter(df["trip_distance"] > 0).\
         filter(df[dropoff_col] > df[pickup_col])

    # Normalizar e validar localizações de embarque e desembarque
    df = df.withColumn("has_problem", when((col("PULocationID") <= 0) | (col("DOLocationID") <= 0), True).otherwise(col("has_problem")))
    df = df.withColumn("problem_description", when((col("PULocationID") <= 0) | (col("DOLocationID") <= 0), concat_ws(";", col("problem_description"), lit("invalid PULocationID or DOLocationID"))).otherwise(col("problem_description")))

  # Dimensões de qualidade para dataset de For Hire Vehicles
  elif taxi_type == 'forHireVehicle':
    df = df.filter(df["PUlocationID"].isNotNull() & df["DOlocationID"].isNotNull())
 
  # Dimensões de qualidade para dataset de For Hire Vehicles (High Volume)
  elif taxi_type == 'highVolumeForHire':
    df = df.filter(df["trip_miles"] > 0).\
         filter(df["trip_time"] > 0).\
         filter(df["dropoff_datetime"] > df["pickup_datetime"])

  return df



# Função principal de transformação
def trusted_transform(month, year, taxi_type_folder, taxi_type_filename):
  filename = f"{taxi_type_filename}_{year}-{month}.parquet"
  path_filename = f"{taxi_type_folder}/{year}/{filename}"
  bucket = "mba-nyc-dataset-f"
  source_key = f"raw/{path_filename}"
  destination_key = f"trusted/{taxi_type_folder}/{year}/{month}/{filename}" # Alteração aqui

  print(f"Iniciando processamento do arquivo: {filename}")
  try:
    start_time = time.time()

    df = spark.read.parquet(f"/content/{taxi_type_filename}_{year}-{month}.parquet").cache()
    # df = spark.read.parquet(f"s3a://{bucket}/{source_key}").cache()
    total_rows = df.count()
    print(f"Arquivo carregado com sucesso: {filename} | Total de linhas: {total_rows}")
    print('Dataframe antes dos tratamentos:')
    df.show(10)


    df_cleaned = apply_quality_dimensions(df, taxi_type_folder)

    print('Dataframe após os tratamentos:')
    df_cleaned.show(10)
    # return df_cleaned
    # df_cleaned \
    #     .repartition(4) \
    #     .write \
    #     .mode("overwrite") \
    #     .parquet(f"s3a://{bucket}/{destination_key}")

    print(f"✅ Arquivo salvo com sucesso em: trusted/{taxi_type_folder}/{year}/{month}/{filename}") # alteração aqui
    print(f"Tempo de execução: {round(time.time() - start_time, 2)} segundos")

    return df_cleaned

  except Exception as e:
    print(f"❌ Falha ao processar o arquivo {filename}: {str(e)}")
