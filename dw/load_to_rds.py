from pyspark.sql import SparkSession

# Conexão RDS MySQL
jdbc_url = "jdbc:mysql://nyc-dw-mysql.coseekllgrql.us-east-1.rds.amazonaws.com:3306/nyc_dw"
jdbc_props = {
    "user": "admin",
    "password": "SuaSenhaForte123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Caminho do JAR MySQL no S3
jar_path = "s3://mba-nyc-dataset/emr/jars/mysql-connector-j-8.0.33.jar"

# Inicializa Spark no EMR
spark = SparkSession.builder \
    .appName("Export to RDS - EMR") \
    .config("spark.jars", jar_path) \
    .getOrCreate()

# Lista de tabelas a exportar
tables = [
    "fact_taxi_trip", "dim_time", "dim_service_type", "dim_location",
    "dim_payment_type", "dim_vendor", "dim_ratecode"
]

base_path = "s3a://mba-nyc-dataset/dw/"

for table in tables:
    print(f"📤 Exportando tabela: {table}")
    df = spark.read.parquet(f"{base_path}{table}")
    df.write.jdbc(url=jdbc_url, table=table, mode="overwrite", properties=jdbc_props)

print("✅ Exportação para o RDS concluída com sucesso!")
