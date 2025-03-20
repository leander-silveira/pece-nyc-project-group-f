from pyspark.sql import SparkSession

def read_parquet_from_s3():
    # Inicializa a Spark Session
    spark = SparkSession.builder.appName("ReadS3Parquet").getOrCreate()
    
    # Caminho do arquivo no S3
    s3_path = "s3://mba-nyc-dataset/raw/yellow/2024/yellow_tripdata_2024-12.parquet"
    
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
