import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat_ws
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# Inicializa Spark no EMR
spark = SparkSession.builder \
    .appName("NYC Taxi Trusted Transform") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
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
