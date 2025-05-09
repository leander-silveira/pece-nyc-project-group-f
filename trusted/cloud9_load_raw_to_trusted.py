import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat_ws

# Corrigindo o erro  `java.lang.RuntimeException: java.lang.ClassNotFoundException: Class org.apache.hadoop.fs.s3a.S3AFileSystem not found`:

# mkdir -p ~/spark_jars
# cd ~/spark_jars
# wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar
# wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar


#Corrigir erro `Cannot locate configuration: tried hadoop-metrics2-s3a-file-system.properties, hadoop-metrics2.properties`:
# touch ~/hadoop-metrics2.properties


# Caminho para JARs no Cloud9
home_dir = os.environ["HOME"]
jars_path = f"{home_dir}/spark_jars/hadoop-aws-3.3.1.jar,{home_dir}/spark_jars/aws-java-sdk-bundle-1.11.901.jar"

# Inicializa Spark
spark = SparkSession.builder \
    .appName("NYC Taxi Trusted Transform") \
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

spark.sparkContext.setLogLevel("WARN")

# Mapeamento dos tipos de táxi
TAXI_TYPES = {
    'fhv_tripdata': 'forHireVehicle',
    'green_tripdata': 'greenTaxi',
    'yellow_tripdata': 'yellowTaxi',
    'fhvhv_tripdata': 'highVolumeForHire',
}

def apply_cleaning_rules(df, taxi_type):
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

def main(month, year, taxi_type_folder, taxi_type_filename):
    filename = f"{taxi_type_filename}_{year}-{month}.parquet"
    source_path = f"s3a://mba-nyc-dataset/raw/{taxi_type_folder}/{year}/{filename}"
    destination_path = f"s3a://mba-nyc-dataset/trusted/{taxi_type_folder}/"

    print(f"\n🔄 Processando arquivo: {filename}")
    try:
        start = time.time()
        df = spark.read.parquet(source_path)

        print(f"📥 Linhas lidas: {df.count()}")
        df_cleaned = apply_cleaning_rules(df, taxi_type_folder)

        # Salva com partição por ano/mês
        df_cleaned \
            .withColumn("year", lit(int(year))) \
            .withColumn("month", lit(int(month))) \
            .coalesce(4) \
            .write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(destination_path)

        print(f"✅ Salvo em: {destination_path} (particionado por year/month)")
        print(f"⏱️ Tempo de execução: {round(time.time() - start, 2)}s")

    except Exception as e:
        print(f"❌ Erro ao processar {filename}: {e}")

# Loop principal
months = [f"{m:02d}" for m in range(1, 13)]
years = [2022, 2023, 2024]

for year in years:
    for month in months:
        for taxi_type_filename, taxi_type_folder in TAXI_TYPES.items():
            main(
                month=month,
                year=year,
                taxi_type_folder=taxi_type_folder,
                taxi_type_filename=taxi_type_filename
            )
