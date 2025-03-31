import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat_ws
import boto3

# Inicializa sessão Spark no EMR
spark = SparkSession.builder \
    .appName("NYC Taxi Trusted Transform") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "6") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Mapeamento dos tipos de táxi
TAXI_TYPES = {
    'fhv_tripdata': 'forHireVehicle',
    'green_tripdata': 'greenTaxi',
    'yellow_tripdata': 'yellowTaxi',
    'fhvhv_tripdata': 'highVolumeForHire',
}

# Conexão com boto3 para verificar existência do arquivo
s3_client = boto3.client("s3")
BUCKET_NAME = "mba-nyc-dataset"

def already_processed(taxi_type_folder, year, month):
    prefix = f"clean/{taxi_type_folder}/year={year}/month={int(month)}"
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    return 'Contents' in response

# Função de limpeza
def apply_cleaning_rules(df, taxi_type):
    print(f"✔ Limpando tipo: {taxi_type}")
    df = df.withColumn("has_problem", lit(False))
    df = df.withColumn("problem_description", lit(""))

    if taxi_type in ['yellowTaxi', 'greenTaxi']:
        pickup = 'tpep_pickup_datetime' if taxi_type == 'yellowTaxi' else 'lpep_pickup_datetime'
        dropoff = 'tpep_dropoff_datetime' if taxi_type == 'yellowTaxi' else 'lpep_dropoff_datetime'

        df = df.withColumn("has_problem", when(col("passenger_count") <= 0, True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when(col("passenger_count") <= 0, concat_ws(";", col("problem_description"), lit("passenger_count <= 0"))).otherwise(col("problem_description")))

        df = df.withColumn("has_problem", when(col("trip_distance") <= 0, True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when(col("trip_distance") <= 0, concat_ws(";", col("problem_description"), lit("trip_distance <= 0"))).otherwise(col("problem_description")))

        df = df.withColumn("has_problem", when(col(dropoff) <= col(pickup), True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when(col(dropoff) <= col(pickup), concat_ws(";", col("problem_description"), lit("dropoff <= pickup"))).otherwise(col("problem_description")))

    elif taxi_type == 'forHireVehicle':
        df = df.withColumn("has_problem", when(col("PUlocationID").isNull() & col("DOlocationID").isNull(), True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when(col("PUlocationID").isNull() & col("DOlocationID").isNull(), concat_ws(";", col("problem_description"), lit("PU and DO missing"))).otherwise(col("problem_description")))

    elif taxi_type == 'highVolumeForHire':
        df = df.withColumn("has_problem", when(col("trip_miles") <= 0, True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when(col("trip_miles") <= 0, concat_ws(";", col("problem_description"), lit("trip_miles <= 0"))).otherwise(col("problem_description")))

        df = df.withColumn("has_problem", when(col("trip_time") <= 0, True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when(col("trip_time") <= 0, concat_ws(";", col("problem_description"), lit("trip_time <= 0"))).otherwise(col("problem_description")))

        df = df.withColumn("has_problem", when(col("dropoff_datetime") <= col("pickup_datetime"), True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when(col("dropoff_datetime") <= col("pickup_datetime"), concat_ws(";", col("problem_description"), lit("dropoff <= pickup"))).otherwise(col("problem_description")))

    return df

# Função principal de transformação
def trusted_transform(month, year, taxi_type_folder, taxi_type_filename):
    filename = f"{taxi_type_filename}_{year}-{month}.parquet"
    source_path = f"s3a://mba-nyc-dataset/raw/{taxi_type_folder}/{year}/{filename}"
    destination_path = f"s3a://mba-nyc-dataset/clean/{taxi_type_folder}/"

    if already_processed(taxi_type_folder, year, month):
        print(f"⏩ Pulando {filename} (já processado)")
        return

    print(f"\n🔄 Processando: {filename}")
    try:
        start = time.time()
        df = spark.read.parquet(source_path)

        df_cleaned = apply_cleaning_rules(df, taxi_type_folder) \
            .withColumn("year", lit(int(year))) \
            .withColumn("month", lit(int(month))) \
            .repartition(8, "year", "month")

        df_cleaned.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(destination_path)

        print(f"✅ Salvo em: {destination_path}")
        print(f"⏱️ Duração: {round(time.time() - start, 2)}s")

    except Exception as e:
        print(f"❌ Erro com {filename}: {e}")

# Loop principal
months = [f"{m:02d}" for m in range(1, 13)]
years = [2022, 2023, 2024]

for year in years:
    for month in months:
        for taxi_type_filename, taxi_type_folder in TAXI_TYPES.items():
            trusted_transform(
                month=month,
                year=year,
                taxi_type_folder=taxi_type_folder,
                taxi_type_filename=taxi_type_filename
            )
