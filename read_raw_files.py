import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, current_timestamp, lower, lit
from pyspark.sql.types import IntegerType, FloatType, StringType, TimestampType



def read_parquet_from_s3():
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
    
    # Caminho do arquivo no S3
    s3_path = "s3a://mba-nyc-dataset/raw/yellow/2024/yellow_tripdata_2024-12.parquet"
    
    # Lê o arquivo Parquet do S3
    df = spark.read.parquet(s3_path)
    
    # Exibe o schema do DataFrame
    df.printSchema()
    
    # Exibe as primeiras linhas do DataFrame
    df.show(10)
    
    return df

# Exemplo de uso
if __name__ == "__main__":
    df = read_parquet_from_s3()
