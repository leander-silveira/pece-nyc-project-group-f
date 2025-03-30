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


# Definir os caminhos no S3
trusted_path = "s3a://mba-nyc-dataset/trusted/all_taxi_trips/"
dw_path = "s3a://mba-nyc-dataset/dw/"

# Ler os dados da camada Trusted (Parquet)
df = spark.read.parquet(trusted_path)

# Verificar se há dados antes de prosseguir
if df.isEmpty():
    print("Nenhum dado encontrado na camada Trusted. Processo encerrado.")
else:
    # Remover duplicatas e valores nulos
    df_transformed = df.dropDuplicates().dropna()

    # Opcional: Converter colunas para tipos apropriados (ajuste conforme necessário)
    df_transformed = df_transformed.withColumn("trip_distance", col("trip_distance").cast("double")) \
                                   .withColumn("fare_amount", col("fare_amount").cast("double")) \
                                   .withColumn("passenger_count", col("passenger_count").cast("int"))

    # Gravar os dados na camada DW (Gold)
    df_transformed.write.mode("overwrite").parquet(dw_path)

    print("Processo concluído. Dados movidos para a camada DW com sucesso.")

# Encerrar a sessão Spark
spark.stop()
