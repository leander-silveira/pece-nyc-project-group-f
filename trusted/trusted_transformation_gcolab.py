import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat_ws
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

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
    'green_tripdata': 'greenTaxi',
    'yellow_tripdata': 'yellowTaxi',
    'fhv_tripdata': 'forHireVehicle',
    'fhvhv_tripdata': 'highVolumeForHire',
}

# Criar DataFrame de pagamento em PySpark
schema_payment = StructType([
    StructField("payment_type", IntegerType(), True),
    StructField("payment_type_description", StringType(), True)
])

data_payment = [
    (1, "Credit card"),
    (2, "Cash"),
    (3, "No charge"),
    (4, "Dispute"),
    (5, "Unknown"),
    (6, "Voided trip")
]

df_dim_payment_type = spark.createDataFrame(data_payment, schema=schema_payment)

# Função para aplicar regras de limpeza
def apply_cleaning_rules(df, taxi_type):
    print(f"✔ Aplicando regras de limpeza para tipo: {taxi_type}")
    df = df.withColumn("has_problem", lit(False))
    df = df.withColumn("problem_description", lit(""))
    miles_to_km = 1.60934

    # Dimensões de qualidade para dataset de taxis verdes e amarelos
    if taxi_type in ['yellowTaxi', 'greenTaxi']:
        pickup_col = 'tpep_pickup_datetime' if taxi_type == 'yellowTaxi' else 'lpep_pickup_datetime'
        dropoff_col = 'tpep_dropoff_datetime' if taxi_type == 'yellowTaxi' else 'lpep_dropoff_datetime'

        # Verificação de valores inválidos
        df = df.withColumn("passenger_count",when(col("passenger_count")<=0, lit(1)).otherwise(col("passenger_count")))

        # Filtra apenas linhas que contém distância de viagens maior ou igual  0; Dados em que desembarque é maior que embarque
        df = df.filter(df["trip_distance"] > 0).\
                filter(df[dropoff_col] > df[pickup_col])

        # Normalizar e validar localizações de embarque e desembarque
        df = df.withColumn("has_problem", when((col("PULocationID") <= 0) | (col("DOLocationID") <= 0), True).otherwise(col("has_problem")))
        df = df.withColumn("problem_description", when((col("PULocationID") <= 0) | (col("DOLocationID") <= 0), concat_ws(";", col("problem_description"), lit("invalid PULocationID or DOLocationID"))).otherwise(col("problem_description")))

        # Transformando payment_type para número inteiro
        df = df.withColumn("payment_type", col("payment_type").cast(IntegerType()))

        # Adicionando descrição do payment_type
        df = df.join(
          df_dim_payment_type, on="payment_type", how="left"
        )

        # Adicionando coluna de distância em km
        df = df.withColumn("trip_distance_km", col("trip_distance") * lit(miles_to_km))


        ### Renomeando colunas
        df = df.withColumnRenamed(pickup_col, "pickup_datetime")
        df = df.withColumnRenamed(dropoff_col, "dropoff_datetime")

    # Dimensões de qualidade para dataset de For Hire Vehicles
    elif taxi_type == 'forHireVehicle':
        # Substitui nulo por 0 nos casos de corridas não compartilhadas
        df = df.withColumn("SR_Flag", when(col("SR_Flag").isNull(), lit(0)).otherwise(col("SR_Flag")))

    # Dimensões de qualidade para dataset de For Hire Vehicles (High Volume)
      # Filtra viagens com distância, tempo maior que zero; filtra viagens que data e hora do desembarque seja maior que embarque
    elif taxi_type == 'highVolumeForHire':
        # Adicionando coluna de distância em km
        df = df.withColumn("trip_miles_km", col("trip_miles") * lit(miles_to_km))
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

        df = spark.read.parquet(f"/content/{taxi_type_filename}_{year}-{month}.parquet").cache() # df = spark.read.parquet(f"s3a://{bucket}/{source_key}").cache()
        total_rows = df.count()
        print(f'Total de linhas antes do tratamento: {total_rows}')

        # Aplicando transformações
        df_cleaned = apply_quality_dimensions(df, taxi_type_folder)
        total_rows_cleaned = df_cleaned.count()
        total_rows_diff = total_rows - total_rows_cleaned
        print(f'Total de linhas após o tratamento: {total_rows_cleaned}')
        print(f'Total de linhas removidas: {total_rows_diff} ({round(total_rows_diff/total_rows*100,2)}%)')


        print('============ Dataframe ANTES do tratamento ============')
        df.show(5)
        print('============ Dataframe DEPOIS do tratamento ============')
        df_cleaned.show(5)

        # df_cleaned \
        #     .repartition(4) \
        #     .write \
        #     .mode("overwrite") \
        #     .parquet(f"s3a://{bucket}/{destination_key}")

        print(f"✅ Arquivo salvo com sucesso em: trusted/{taxi_type_folder}/{year}/{month}/{filename}") # alteração aqui
        print(f"Tempo de execução: {round(time.time() - start_time, 2)} segundos")
        print('==============================================================')

        return df_cleaned

    except Exception as e:
        print(f"❌ Falha ao processar o arquivo {filename}: {str(e)}")

# Loop principal (anos fixos de 2022 a 2024)
months = [f"{m:02d}" for m in range(1, 2)] # [f"{m:02d}" for m in range(1, 13)]
years = [2024] # [2022, 2023, 2024]

for year in years:
    for month in months:
        for taxi_type_filename, taxi_type_folder in TAXI_TYPES.items():
            print(f"\n>>> Processando: {taxi_type_filename} | Ano: {year} | Mês: {month}")
            trusted_transform(
                month=month,
                year=year,
                taxi_type_folder=taxi_type_folder,
                taxi_type_filename=taxi_type_filename
            )
