import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Definir caminho correto para os JARs
home_dir = os.environ["HOME"]

# Criar sessão Spark
spark = SparkSession.builder \
    .appName("NYC Taxi Data Processing") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.jars", f"{home_dir}/hadoop-aws-3.3.1.jar,{home_dir}/aws-java-sdk-bundle-1.11.901.jar") \
    .getOrCreate()

# Caminhos S3
raw_bucket = "s3a://mba-nyc-dataset/raw/"
trusted_bucket = "s3a://mba-nyc-dataset/trusted/"

# Definir schema manualmente
schema = StructType([
    StructField("vendor_id", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

# Ler dados do S3 com schema definido
df = spark.read.schema(schema).parquet(raw_bucket)

# Tratamento dos dados
df_cleaned = df \
    .withColumn("passenger_count", when(col("passenger_count").isNull(), 1).otherwise(col("passenger_count"))) \
    .withColumn("trip_distance", when(col("trip_distance").isNull(), 0.0).otherwise(col("trip_distance"))) \
    .dropDuplicates()

# Enriquecimento
df_enriched = df_cleaned.withColumn("processing_timestamp", current_timestamp())

# Salvar no S3
df_enriched.write.mode("overwrite").parquet(trusted_bucket)

print("Processamento concluído!")
