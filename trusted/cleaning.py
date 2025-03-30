import os
import boto3
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, current_timestamp, lower, lit
from pyspark.sql.types import IntegerType, FloatType, StringType, TimestampType

# Definir caminho correto para os JARs no Cloud9
home_dir = os.environ["HOME"]
jars_path = f"{home_dir}/spark_jars/hadoop-aws-3.3.1.jar,{home_dir}/spark_jars/aws-java-sdk-bundle-1.11.901.jar"

# Inicializa Spark
spark = SparkSession.builder \
    .appName("NYC Taxi Data Processing") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.jars", jars_path) \
    .getOrCreate()
    
# Mapeamento dos tipos de táxi
TAXI_TYPES = {
    'fhv_tripdata': 'forHireVehicle',
    'green_tripdata': 'greenTaxi',
    'yellow_tripdata': 'yellowTaxi',
    'fhvhv_tripdata': 'highVolumeForHire',
}

# Cria função para aplicar validações

def apply_cleaning_rules(df, taxi_type):
    problems = []

    if taxi_type in ['yellowTaxi', 'greenTaxi']:
        df = df.withColumn("has_problem", lit(False))
        df = df.withColumn("problem_description", lit(""))

        df = df.withColumn("has_problem", when(col("passenger_count") <= 0, True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when(col("passenger_count") <= 0, concat_ws(";", col("problem_description"), lit("passenger_count <= 0"))).otherwise(col("problem_description")))

        df = df.withColumn("has_problem", when(col("trip_distance") <= 0, True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when(col("trip_distance") <= 0, concat_ws(";", col("problem_description"), lit("trip_distance <= 0"))).otherwise(col("problem_description")))

        df = df.withColumn("has_problem", when(col("tpep_dropoff_datetime") <= col("tpep_pickup_datetime"), True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when(col("tpep_dropoff_datetime") <= col("tpep_pickup_datetime"), concat_ws(";", col("problem_description"), lit("dropoff <= pickup"))).otherwise(col("problem_description")))

    elif taxi_type == 'forHireVehicle':
        df = df.withColumn("has_problem", lit(False))
        df = df.withColumn("problem_description", lit(""))

        df = df.withColumn("has_problem", when(col("PUlocationID").isNull() & col("DOlocationID").isNull(), True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when(col("PUlocationID").isNull() & col("DOlocationID").isNull(), concat_ws(";", col("problem_description"), lit("PU and DO missing"))).otherwise(col("problem_description")))

    elif taxi_type == 'highVolumeForHire':
        df = df.withColumn("has_problem", lit(False))
        df = df.withColumn("problem_description", lit(""))

        df = df.withColumn("has_problem", when(col("trip_miles") <= 0, True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when(col("trip_miles") <= 0, concat_ws(";", col("problem_description"), lit("trip_miles <= 0"))).otherwise(col("problem_description")))

        df = df.withColumn("has_problem", when(col("trip_time") <= 0, True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when(col("trip_time") <= 0, concat_ws(";", col("problem_description"), lit("trip_time <= 0"))).otherwise(col("problem_description")))

        df = df.withColumn("has_problem", when(col("dropoff_datetime") <= col("pickup_datetime"), True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when(col("dropoff_datetime") <= col("pickup_datetime"), concat_ws(";", col("problem_description"), lit("dropoff <= pickup"))).otherwise(col("problem_description")))

    return df

# Função principal de transformação

def trusted_transform(s3, month, year, taxi_type_folder, taxi_type_filename):
    filename = f"{taxi_type_filename}_{year}-{month}.parquet"
    path_filename = f"{taxi_type_folder}/{year}/{filename}"
    bucket = "mba-nyc-dataset"
    source_key = f"raw/{path_filename}"
    destination_key = f"trusted/{path_filename}"

    # Lê arquivo do S3
    try:
        df = spark.read.parquet(f"s3a://{bucket}/{source_key}")
        df_cleaned = apply_cleaning_rules(df, taxi_type_folder)
        df_cleaned.write.mode("overwrite").parquet(f"s3a://{bucket}/{destination_key}")
        print(f"Arquivo {filename} processado com sucesso e salvo na pasta trusted/{taxi_type_folder}")
    except Exception as e:
        print(f"Falha ao processar o arquivo {filename}: {e}")

# Execução do loop principal

s3 = boto3.client("s3")
today = datetime.now()
today_year = today.year
months = ['0' + str(m) if m < 10 else str(m) for m in range(1, 13)]
years = list(range(2022, today_year + 1))

for year in years:
    for month in months:
        for taxi_type_filename, taxi_type_folder in TAXI_TYPES.items():
            trusted_transform(
                s3=s3,
                month=month,
                year=year,
                taxi_type_folder=taxi_type_folder,
                taxi_type_filename=taxi_type_filename
            )
