import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat_ws

# Inicializa Spark no EMR
spark = SparkSession.builder \
    .appName("NYC Taxi Trusted Transform") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Mapeamento dos tipos de táxi
TAXI_TYPES = {
    'fhv_tripdata': 'forHireVehicle',
    'green_tripdata': 'greenTaxi',
    'yellow_tripdata': 'yellowTaxi',
    'fhvhv_tripdata': 'highVolumeForHire',
}

# Função para aplicar regras de limpeza
def apply_cleaning_rules(df, taxi_type):
    print(f"✔ Aplicando regras de limpeza para tipo: {taxi_type}")
    df = df.withColumn("has_problem", lit(False))
    df = df.withColumn("problem_description", lit(""))

    if taxi_type in ['yellowTaxi', 'greenTaxi']:
        pickup_col = 'tpep_pickup_datetime' if taxi_type == 'yellowTaxi' else 'lpep_pickup_datetime'
        dropoff_col = 'tpep_dropoff_datetime' if taxi_type == 'yellowTaxi' else 'lpep_dropoff_datetime'

        df = df.withColumn("has_problem", when(col("passenger_count") <= 0, True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when(col("passenger_count") <= 0, concat_ws(";", col("problem_description"), lit("passenger_count <= 0"))).otherwise(col("problem_description")))

        df = df.withColumn("has_problem", when(col("trip_distance") <= 0, True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when(col("trip_distance") <= 0, concat_ws(";", col("problem_description"), lit("trip_distance <= 0"))).otherwise(col("problem_description")))

        df = df.withColumn("has_problem", when(col(dropoff_col) <= col(pickup_col), True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when(col(dropoff_col) <= col(pickup_col), concat_ws(";", col("problem_description"), lit("dropoff <= pickup"))).otherwise(col("problem_description")))

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
            trusted_transform(
                month=month,
                year=year,
                taxi_type_folder=taxi_type_folder,
                taxi_type_filename=taxi_type_filename
            )
