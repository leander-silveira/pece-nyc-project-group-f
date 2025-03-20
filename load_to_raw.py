import requests
from bs4 import BeautifulSoup
import boto3
from tqdm import tqdm
from io import BytesIO
import re
import time  # Para retries
import json

# Configurações do S3
S3_BUCKET = "mba-nyc-dataset"
S3_PREFIX = "raw"

# URL da página de datasets
BASE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# Inicializa cliente S3
s3 = boto3.client("s3")

# Tipos de datasets disponíveis
TAXI_TYPES = ["yellow", "green", "fhv", "hvfhv"]

# Lista de arquivos que falharam no processamento
failed_uploads = []

def get_download_links():
    print("🔍 Extraindo links da página oficial...")
    attempts = 5
    for attempt in range(attempts):
        try:
            response = requests.get(BASE_URL, timeout=10)
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            print(f"⚠ Erro ao carregar a página (tentativa {attempt + 1}/{attempts}): {e}")
            if attempt < attempts - 1:
                time.sleep(2)
            else:
                raise RuntimeError("❌ Falha ao carregar a página após múltiplas tentativas")
    
    soup = BeautifulSoup(response.text, "html.parser")
    links = {}
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "tripdata" in href and href.endswith(".parquet"):
            full_url = href if href.startswith("http") else f"https://www.nyc.gov{href}"
            filename = href.split("/")[-1]
            taxi_type, year, month = extract_metadata(filename)
            if taxi_type and year and month:
                dataset_key = f"{taxi_type}_tripdata_{year}-{month}.parquet"
                links[dataset_key] = full_url
    
    print(f"✅ {len(links)} datasets encontrados!")
    return links

def extract_metadata(filename):
    match = re.search(r"(yellow|green|fhv|hvfhv)_tripdata_(\d{4})-(\d{2})", filename, re.IGNORECASE)
    return match.groups() if match else (None, None, None)

def download_and_upload_to_s3(url, taxi_type, year, filename):
    s3_key = f"{S3_PREFIX}/{taxi_type}/{year}/{filename}"
    
    print(f"⬇ Baixando {filename}...")
    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        total_size = int(response.headers.get("content-length", 0))
        total_size_mb = total_size / (1024 * 1024)
        print(f"📥 Tamanho do arquivo: {total_size_mb:.2f} MB")
        progress_bar = tqdm(total=total_size_mb, unit="MB", unit_scale=True, desc="Baixando", colour="green")
        
        file_data = BytesIO()
        for chunk in response.iter_content(chunk_size=8192):
            file_data.write(chunk)
            progress_bar.update(len(chunk) / (1024 * 1024))
        
        progress_bar.close()
        file_data.seek(0)
        
        print(f"🚀 Enviando {filename} para s3://{S3_BUCKET}/{s3_key}...")
        s3.upload_fileobj(file_data, S3_BUCKET, s3_key)
        print(f"✅ Upload concluído: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"❌ Erro ao processar {filename}: {e}")
        failed_uploads.append({"taxi_type": taxi_type, "year": year, "filename": filename, "error": str(e)})

def main(years=["2023", "2024"], month=None):
    print(f"📅 Buscando datasets para {years} e mês: {month if month else 'todos'}...")
    all_links = get_download_links()
    
    for taxi_type in TAXI_TYPES:
        for year in years:
            for m in range(1, 13):
                month_str = f"{m:02d}"
                dataset_key = f"{taxi_type}_tripdata_{year}-{month_str}.parquet"
                if month and month_str != month:
                    continue
                
                if dataset_key in all_links:
                    download_and_upload_to_s3(all_links[dataset_key], taxi_type, year, dataset_key)
    
    if failed_uploads:
        print("⚠ Alguns arquivos falharam no processamento. Salvando lista no S3...")
        failed_json = json.dumps(failed_uploads, indent=2)
        failed_key = f"{S3_PREFIX}/failed_uploads.json"
        s3.put_object(Bucket=S3_BUCKET, Key=failed_key, Body=failed_json, ContentType="application/json")
        print(f"🚨 Lista de falhas salva em s3://{S3_BUCKET}/{failed_key}")
    
    print("✅ Processamento concluído!")


if __name__ == "__main__":
    main()
