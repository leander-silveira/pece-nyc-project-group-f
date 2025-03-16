import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, current_timestamp, lower

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

print("✅ Sessão Spark iniciada!")

# Caminhos S3
raw_bucket = "s3a://mba-nyc-dataset/raw"
trusted_bucket = "s3a://mba-nyc-dataset/trusted"

# Configuração para ler múltiplos anos e meses
years = ["2023", "2024"]
months = [f"{m:02d}" for m in range(1, 13)]
taxi_types = ["yellow", "green", "fhv", "hvfhv"]

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
                if "VendorID" in columns:
                    df = df.withColumnRenamed("VendorID", "vendor_id")
                elif "dispatching_base_num" in columns:
                    df = df.withColumnRenamed("dispatching_base_num", "vendor_id")
                else:
                    print(f"⚠ Dataset {taxi_type} {year}-{month} não contém 'vendor_id'. Continuando sem modificar.")
                
                # Corrigir os tipos dos dados necessários
                df = df.withColumn("passenger_count", col("passenger_count").cast("int")) \
                       .withColumn("trip_distance", col("trip_distance").cast("float")) \
                       .withColumn("fare_amount", col("fare_amount").cast("float")) \
                       .withColumn("total_amount", col("total_amount").cast("float"))

                # Tratamento dos dados
                df_cleaned = df \
                    .withColumn("vendor_id", trim(lower(col("vendor_id")))) if "vendor_id" in df.columns else df \
                    .withColumn("passenger_count", when(col("passenger_count").isNull(), 1).otherwise(col("passenger_count"))) \
                    .withColumn("trip_distance", when(col("trip_distance").isNull(), 0.0).otherwise(col("trip_distance"))) \
                    .dropDuplicates()

                # Enriquecimento
                df_enriched = df_cleaned.withColumn("processing_timestamp", current_timestamp())

                # Definir caminho para salvar na camada trusted
                trusted_path = f"{trusted_bucket}/{year}/{month}/{taxi_type}_tripdata_trusted_{year}-{month}.parquet"
                
                print(f"🚀 Salvando dados limpos em {trusted_path}...")
                df_enriched.write.mode("overwrite").parquet(trusted_path)

                print(f"✅ Processamento concluído para {taxi_type} {year}-{month}!")
            
            except Exception as e:
                print(f"❌ Erro ao processar {taxi_type} {year}-{month}: {e}")

print("🎉 Processamento finalizado para todos os arquivos!")
