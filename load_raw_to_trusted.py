import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, current_timestamp, lower, lit
from pyspark.sql.types import IntegerType, FloatType, StringType, TimestampType

# Definir caminho correto para os JARs no Cloud9
home_dir = os.environ["HOME"]
jars_path = f"{home_dir}/spark_jars/hadoop-aws-3.3.1.jar,{home_dir}/spark_jars/aws-java-sdk-bundle-1.11.901.jar"

# Criar sessão Spark com suporte ao S3 no Cloud9
spark = SparkSession.builder \
    .appName("NYC Taxi Data Processing") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.jars", jars_path) \
    .getOrCreate()

# 🚀 Ativar configuração para evitar `_temporary/`
spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")

print("✅ Sessão Spark iniciada!")

# Caminhos S3
raw_bucket = "s3a://mba-nyc-dataset/raw"
trusted_bucket = "s3a://mba-nyc-dataset/trusted/all_taxi_trips"

# Configuração para ler múltiplos anos e meses
years = ["2023", "2024"]
months = [f"{m:02d}" for m in range(1, 13)]
taxi_types = ["yellow", "green", "fhv", "hvfhv"]

# Lista para armazenar todos os DataFrames antes de unir
all_taxi_dfs = []

# Schema final padronizado para unificação
final_schema = {
    "vendor_id": StringType(),
    "pickup_datetime": TimestampType(),
    "dropoff_datetime": TimestampType(),
    "passenger_count": IntegerType(),
    "trip_distance": FloatType(),
    "fare_amount": FloatType(),
    "total_amount": FloatType(),
    "PULocationID": IntegerType(),
    "DOLocationID": IntegerType(),
    "taxi_type": StringType(),
    "processing_timestamp": TimestampType()
}

# Processar arquivos por ano, mês e tipo de táxi
for year in years:
    for month in months:
        for taxi_type in taxi_types:
            raw_path = f"{raw_bucket}/{year}/{month}/{taxi_type}_tripdata_{year}-{month}.parquet"
            
            try:
                print(f"📥 Lendo dados de {raw_path}...")

                # 🚀 SOLUÇÃO: Ler sem definir schema (deixar o Spark inferir automaticamente)
                df = spark.read.parquet(raw_path)

                # 🚀 SOLUÇÃO: Verificar colunas disponíveis
                columns = df.columns

                # Padronizar nomes de colunas para consistência
                df = df.withColumnRenamed("VendorID", "vendor_id") if "VendorID" in columns else df
                df = df.withColumnRenamed("dispatching_base_num", "vendor_id") if "dispatching_base_num" in columns else df
                df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") if "tpep_pickup_datetime" in columns else df
                df = df.withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") if "tpep_dropoff_datetime" in columns else df
                df = df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") if "lpep_pickup_datetime" in columns else df
                df = df.withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime") if "lpep_dropoff_datetime" in columns else df
                df = df.withColumnRenamed("dropOff_datetime", "dropoff_datetime") if "dropOff_datetime" in columns else df

                # Adicionar tipo de táxi para identificar depois
                df = df.withColumn("taxi_type", lit(taxi_type))

                # 🚀 SOLUÇÃO: Padronizar colunas ausentes (preencher com valores padrão)
                for col_name, col_type in final_schema.items():
                    if col_name not in df.columns:
                        df = df.withColumn(col_name, lit(None).cast(col_type))

                # Conversão de tipos para manter padronização
                df = df.withColumn("passenger_count", col("passenger_count").cast(IntegerType())) \
                       .withColumn("trip_distance", col("trip_distance").cast(FloatType())) \
                       .withColumn("fare_amount", col("fare_amount").cast(FloatType())) \
                       .withColumn("total_amount", col("total_amount").cast(FloatType())) \
                       .withColumn("PULocationID", col("PULocationID").cast(IntegerType())) \
                       .withColumn("DOLocationID", col("DOLocationID").cast(IntegerType()))

                # Tratamento dos dados
                df_cleaned = df \
                    .withColumn("vendor_id", trim(lower(col("vendor_id")))) if "vendor_id" in df.columns else df \
                    .withColumn("passenger_count", when(col("passenger_count").isNull(), 1).otherwise(col("passenger_count"))) \
                    .withColumn("trip_distance", when(col("trip_distance").isNull(), 0.0).otherwise(col("trip_distance"))) \
                    .dropDuplicates()

                # Enriquecimento
                df_enriched = df_cleaned.withColumn("processing_timestamp", current_timestamp())

                print(f"✅ Processamento concluído para {taxi_type} {year}-{month}!")

                # Adicionar DataFrame à lista para unir depois
                all_taxi_dfs.append(df_enriched)
            
            except Exception as e:
                print(f"❌ Erro ao processar {taxi_type} {year}-{month}: {e}")

# 🚀 Consolidar todos os tipos de táxi em uma única tabela
if all_taxi_dfs:
    print("🚀 Consolidando todos os táxis em uma única tabela...")

    # Unir todos os DataFrames em um único DataFrame
    all_taxi_df = all_taxi_dfs[0]

    for df in all_taxi_dfs[1:]:
        all_taxi_df = all_taxi_df.unionByName(df, allowMissingColumns=True)  # Permite colunas ausentes

    # 🚀 Salvar tabela unificada sem `_temporary/`
    print(f"🚀 Salvando tabela consolidada em {trusted_bucket}...")
    all_taxi_df.coalesce(1).write.mode("overwrite").parquet(trusted_bucket)

    print("✅ Tabela única de táxis criada com sucesso!")

print("🎉 Processamento finalizado para todos os arquivos!")
