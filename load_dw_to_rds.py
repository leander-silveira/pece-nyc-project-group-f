from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, current_timestamp, lower, lit
from pyspark.sql.types import IntegerType, FloatType, StringType, TimestampType

# Definir caminho correto para os JARs no Cloud9
home_dir = os.environ["HOME"]
jars_path = f"{home_dir}/spark_jars/hadoop-aws-3.3.1.jar,{home_dir}/spark_jars/aws-java-sdk-bundle-1.11.901.jar"




# Criar sessão do Spark com suporte a S3 e JDBC
spark = SparkSession.builder \
    .appName("S3 DW to RDS") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.parquet.mergeSchema", "false") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.files.ignoreCorruptFiles", "true") \
    .config("spark.jars", jars_path) \
    .getOrCreate()

# Definir caminhos e configurações
s3_path = "s3a://mba-nyc-dataset/dw/taxis/"
jdbc_url = "jdbc:mysql://database-1.coseekllgrql.us-east-1.rds.amazonaws.com:3306/database-1"
db_table = "taxi_trips"
db_user = "admin"  # Substitua pelo usuário correto
db_password = "pecenycgrupof"  # Substitua pela senha correta (não recomendado hardcode)

# Ler os dados da camada DW (Parquet)
df = spark.read.parquet(s3_path)

# Verificar se há dados antes de continuar
if df.isEmpty():
    print("Nenhum dado encontrado na camada DW. Processo encerrado.")
else:
    # Remover duplicatas e valores nulos
    df_transformed = df.dropDuplicates().dropna()

    # Ajustar tipos de colunas conforme o schema do RDS (ajuste conforme necessário)
    df_transformed = df_transformed.withColumn("trip_distance", col("trip_distance").cast("double")) \
                                   .withColumn("fare_amount", col("fare_amount").cast("double")) \
                                   .withColumn("passenger_count", col("passenger_count").cast("int"))

    # Escrever os dados no RDS usando JDBC
    df_transformed.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", db_table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()

    print("Dados movidos com sucesso para o RDS.")

# Encerrar a sessão Spark
spark.stop()
