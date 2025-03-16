import requests
from bs4 import BeautifulSoup
import boto3
from tqdm import tqdm
from io import BytesIO
import re

# Configurações do S3
S3_BUCKET = "mba-nyc-dataset"
S3_PREFIX = "raw"

# URL da página de datasets
BASE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# Inicializa cliente S3
s3 = boto3.client("s3")

# Função para extrair os links dos datasets
def get_download_links():
    print("🔍 Extraindo links da página oficial...")
    response = requests.get(BASE_URL)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    links = {}

    # Procura links para arquivos CSV e Parquet
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "tripdata" in href and (href.endswith(".parquet") or href.endswith(".csv")):
            full_url = href if href.startswith("http") else f"https://www.nyc.gov{href}"

            # Extrai o ano e o mês do nome do arquivo
            filename = href.split("/")[-1]
            year, month = extract_year_month(filename)

            if year and month:
                links[f"{year}-{month}"] = full_url

    print(f"✅ {len(links)} datasets encontrados!")
    return links

# Função para extrair o ano e o mês do nome do arquivo
def extract_year_month(filename):
    match = re.search(r"(\d{4})-(\d{2})", filename)
    if match:
        return match.groups()
    return None, None

# Função para baixar e enviar o arquivo direto para o S3 com progresso em MB
def download_and_upload_to_s3(url, year, month):
    filename = url.split("/")[-1]
    s3_key = f"{S3_PREFIX}/{year}/{month}/{filename}"

    # Verifica se o arquivo já existe no S3
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        print(f"⏩ {s3_key} já existe no S3, pulando download.")
        return
    except:
        pass  # Continua para fazer o download

    print(f"⬇ Baixando {filename}...")

    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    total_size_mb = total_size / (1024 * 1024)  # Converte bytes para MB

    print(f"📥 Tamanho do arquivo: {total_size_mb:.2f} MB")

    progress_bar = tqdm(total=total_size_mb, unit="MB", unit_scale=True, desc="Baixando", colour="green")

    file_data = BytesIO()
    downloaded_size = 0

    for chunk in response.iter_content(chunk_size=8192):
        file_data.write(chunk)
        downloaded_size += len(chunk) / (1024 * 1024)  # Atualiza o progresso em MB
        progress_bar.update(len(chunk) / (1024 * 1024))

    progress_bar.close()
    file_data.seek(0)  # Volta para o início do arquivo

    print(f"🚀 Enviando {filename} para s3://{S3_BUCKET}/{s3_key}...")

    # Exibir progresso no upload para S3
    def upload_progress(bytes_transferred):
        uploaded_mb = bytes_transferred / (1024 * 1024)
        # print(f"⬆ Upload: {uploaded_mb:.2f} MB de {total_size_mb:.2f} MB")

    s3.upload_fileobj(file_data, S3_BUCKET, s3_key, Callback=upload_progress)

    print(f"✅ Upload concluído: s3://{S3_BUCKET}/{s3_key}")

# Função principal que pode receber ano e mês como parâmetro
def main(years=["2023", "2024"], month=None):
    print(f"📅 Buscando datasets para {years} e mês: {month if month else 'todos'}...")
    
    all_links = get_download_links()

    for year in years:
        for m in range(1, 13):
            month_str = f"{m:02d}"
            dataset_key = f"{year}-{month_str}"

            # Se um mês específico foi passado, filtra apenas ele
            if month and month_str != month:
                continue

            if dataset_key in all_links:
                try:
                    download_and_upload_to_s3(all_links[dataset_key], year, month_str)
                except Exception as e:
                    print(f"❌ Erro ao processar {dataset_key}: {e}")
            else:
                print(f"⚠ Nenhum dataset encontrado para {dataset_key}.")

    print("✅ Processamento concluído!")

# Função AWS Lambda Handler
def lambda_handler(event, context):
    years = event.get("years", ["2023", "2024"])
    month = event.get("month", None)
    
    main(years=years, month=month)
    return {"status": "Concluído", "anos_processados": years, "mes": month}

# Executar diretamente no script
if __name__ == "__main__":
    main()  # Executa com os anos padrão 2023 e 2024
