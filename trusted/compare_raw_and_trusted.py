from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Inicializa Spark
spark = SparkSession.builder \
    .appName("Comparar Raw vs Trusted") \
    .getOrCreate()

# Caminhos
bucket = "mba-nyc-dataset"
filename = "yellow_tripdata_2024-04.parquet"
raw_path = f"s3a://{bucket}/raw/yellowTaxi/2024/{filename}"
trusted_path = f"s3a://{bucket}/trusted/yellowTaxi/2024/04/"

# Lê os DataFrames
df_raw = spark.read.parquet(raw_path)
df_trusted = spark.read.parquet(trusted_path)

# Conta original
print(f"📦 Total linhas raw: {df_raw.count()}")
print(f"📦 Total linhas trusted: {df_trusted.count()}")

# 1️⃣ Linhas removidas (existem em raw, mas não na trusted)
raw_minus_trusted = df_raw.subtract(df_trusted.select(df_raw.columns))
print(f"\n❌ Linhas removidas na trusted: {raw_minus_trusted.count()}")
raw_minus_trusted.show(5, truncate=False)

# 2️⃣ Linhas com problema (has_problem = True)
if "has_problem" in df_trusted.columns:
    problemas = df_trusted.filter(col("has_problem") == True)
    print(f"\n⚠️ Linhas marcadas com problema: {problemas.count()}")
    problemas.select("has_problem", "problem_description").show(5, truncate=False)
else:
    print("\nℹ️ A trusted não contém a coluna 'has_problem'.")

# Encerra sessão
spark.stop()
