from datetime import datetime
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat_ws

# Inicializa Spark no EMR
spark = SparkSession.builder \
    .appName("NYC Taxi Trusted Transform") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .getOrCreate()

# Mapeamento dos tipos de táxi
TAXI_TYPES = {
    'fhv_tripdata': 'forHireVehicle',
    'green_tripdata': 'greenTaxi',
    'yellow_tripdata': 'yellowTaxi',
    'fhvhv_tripdata': 'highVolumeForHire',
}

# Função para aplicar validações de limpeza
def apply_cleaning_rules(df, taxi_type):
    df = df.withColumn("taxi_type", lit(taxi_type))
    df = df.withColumn("trip_distance", when(col("trip_distance").isNull(), 0).otherwise(col("trip_distance")))
    df = df.withColumn("passenger_count", when(col("passenger_count").isNull(), 0).otherwise(col("passenger_count")))
    return df

# Função principal de processamento
def process_taxi_data():
    input_bucket = "s3://mba-nyc-dataset/raw/"
    output_bucket = "s3://mba-nyc-dataset/trusted/"

    for year in range(2022, 2025):  # Anos de 2022 até 2024
        for month in range(1, 13):
            for prefix, taxi_type in TAXI_TYPES.items():
                file_key = f"{prefix}/{prefix}_{year}-{month:02d}.parquet"
                input_path = input_bucket + file_key
                output_path = output_bucket + file_key

                try:
                    df = spark.read.parquet(input_path)
                    cleaned_df = apply_cleaning_rules(df, taxi_type)
                    cleaned_df.write.mode("overwrite").parquet(output_path)
                    print(f"[SUCESSO] {file_key}")
                except Exception as e:
                    print(f"[ERRO] Falha ao processar {file_key}: {e}")

if __name__ == "__main__":
    process_taxi_data()
