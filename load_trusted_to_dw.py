import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, year, month

# Definir caminho correto para os JARs no Cloud9
home_dir = os.environ["HOME"]
jars_path = f"{home_dir}/spark_jars/hadoop-aws-3.3.1.jar,{home_dir}/spark_jars/aws-java-sdk-bundle-1.11.901.jar"

# Criar sessão Spark com suporte ao S3 no Cloud9
spark = SparkSession.builder \
    .appName("NYC Taxi Data Warehouse") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.jars", jars_path) \
    .getOrCreate()

print("✅ Sessão Spark iniciada!")

# Caminhos S3
trusted_path = "s3a://mba-nyc-dataset/trusted/all_taxi_trips.parquet"
gold_path = "s3a://mba-nyc-dataset/dw/gold"

# 🚀 Ler a camada trusted consolidada
print(f"📥 Lendo dados da camada trusted: {trusted_path}")
df = spark.read.parquet(trusted_path)

# 🚀 Criar colunas de ano e mês para agregações
df = df.withColumn("year", year(col("processing_timestamp"))) \
       .withColumn("month", month(col("processing_timestamp")))

# 🔹 1. Criar a tabela `gold_kpis` com estatísticas gerais
gold_kpis = df.groupBy("year", "month", "taxi_type") \
    .agg(
        count("*").alias("total_viagens"),
        avg("trip_distance").alias("media_distancia_km"),
        avg("passenger_count").alias("media_passageiros"),
        sum("total_amount").alias("faturamento_total")
    )

print("🚀 Salvando KPIs em `gold_kpis`")
gold_kpis.write.mode("overwrite").parquet(f"{gold_path}/gold_kpis.parquet")

# 🔹 2. Criar a tabela `gold_faturamento_mensal` para análise de receita
gold_faturamento = df.groupBy("year", "month", "taxi_type") \
    .agg(sum("total_amount").alias("faturamento_total"))

print("🚀 Salvando Faturamento Mensal em `gold_faturamento_mensal`")
gold_faturamento.write.mode("overwrite").parquet(f"{gold_path}/gold_faturamento_mensal.parquet")

# 🔹 3. Criar a tabela `gold_pickup_hotspots` com os locais mais populares para pickup
if "PULocationID" in df.columns:
    gold_pickup_hotspots = df.groupBy("year", "month", "PULocationID") \
        .agg(count("*").alias("total_viagens")) \
        .orderBy(col("total_viagens").desc())

    print("🚀 Salvando Hotspots de Pickup em `gold_pickup_hotspots`")
    gold_pickup_hotspots.write.mode("overwrite").parquet(f"{gold_path}/gold_pickup_hotspots.parquet")

print("🎉 Camada `dw/gold` criada com sucesso!")
