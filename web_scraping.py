import requests
from bs4 import BeautifulSoup
import boto3
from tqdm import tqdm
from io import BytesIO
import re
import time  # Para retries

# Configurações do S3
S3_BUCKET = "mba-nyc-dataset"
S3_PREFIX = "raw"

# URL da página de datasets
BASE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# Inicializa cliente S3
s3 = boto3.client("s3")

# Tipos de datasets disponíveis
TAXI_TYPES = ["yellow", "green", "fhv", "hvfhv"]

# Função para extrair os links dos datasets com retries
def get_download_links():
    print("🔍 Extraindo links da página oficial...")

    attempts = 5  # Número máximo de tentativas
    for attempt in range(attempts):
        try:
            response = requests.get(BASE_URL, timeout=10)  # Adicionado timeout de 10s
            response.raise_for_status()
            break  # Sai do loop se a requisição for bem-sucedida
        except requests.exceptions.RequestException as e:
            print(f"⚠ Erro ao carregar a página (tentativa {attempt + 1}/{attempts}): {e}")
            if attempt < attempts - 1:
                time.sleep(2)  # Aguarda 2 segundos antes de tentar novamente
            else:
                raise RuntimeError("❌ Falha ao carregar a página após múltiplas tentativas")

    soup = BeautifulSoup(response.text, "html.parser")
    links = {}

    # Procura links para arquivos Parquet
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "tripdata" in href and href.endswith(".parquet"):
            full_url = href if href.startswith("http") else f"https://www.nyc.gov{href}"

            # Extrai tipo de táxi, ano e mês do nome do arquivo
            filename = href.split("/")[-1]
            taxi_type, year, month = extract_metadata(filename)

            if taxi_type and year and month:
                dataset_key = f"{taxi_type}_tripdata_{year}-{month}.parquet"
                links[dataset_key] = full_url

    print(f"✅ {len(links)} datasets encontrados!")
    return links

# Função para extrair tipo de táxi, ano e mês do nome do arquivo
def extract_metadata(filename):
    match = re.search(r"(yellow|green|fhv|hvfhv)_tripdata_(\d{4})-(\d{2})", filename, re.IGNORECASE)
    if match:
        return match.groups()
    return None, None, None

# Função para baixar e enviar o arquivo direto para o S3 com progresso em MB
def download_and_upload_to_s3(url, year, month, filename):
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
    for chunk in response.iter_content(chunk_size=8192):
        file_data.write(chunk)
        progress_bar.update(len(chunk) / (1024 * 1024))

    progress_bar.close()
    file_data.seek(0)  # Volta para o início do arquivo

    print(f"🚀 Enviando {filename} para s3://{S3_BUCKET}/{s3_key}...")

    # Exibir progresso no upload para S3
    def upload_progress(bytes_transferred):
        uploaded_mb = bytes_transferred / (1024 * 1024)

    s3.upload_fileobj(file_data, S3_BUCKET, s3_key, Callback=upload_progress)

    print(f"✅ Upload concluído: s3://{S3_BUCKET}/{s3_key}")

# Função principal que pode receber ano e mês como parâmetro
def main(years=["2023", "2024"], month=None):
    print(f"📅 Buscando datasets para {years} e mês: {month if month else 'todos'}...")

    all_links = get_download_links()

    for taxi_type in TAXI_TYPES:
        for year in years:
            for m in range(1, 13):
                month_str = f"{m:02d}"
                dataset_key = f"{taxi_type}_tripdata_{year}-{month_str}.parquet"

                # Se um mês específico foi passado, filtra apenas ele
                if month and month_str != month:
                    continue

                retries = 3  # Número máximo de tentativas por dataset
                for attempt in range(retries):
                    if dataset_key in all_links:
                        try:
                            filename = dataset_key
                            download_and_upload_to_s3(all_links[dataset_key], year, month_str, filename)
                            break  # Sai do loop se a requisição for bem-sucedida
                        except Exception as e:
                            print(f"❌ Erro ao processar {dataset_key} (tentativa {attempt + 1}/{retries}): {e}")
                            if attempt < retries - 1:
                                time.sleep(5)  # Aguarda 5 segundos antes de tentar novamente
                            else:
                                print(f"⚠ Falha ao baixar {dataset_key} após {retries} tentativas.")
                    else:
                        print(f"⚠ Nenhum dataset encontrado para {dataset_key}. Tentando novamente em 5 segundos...")
                        time.sleep(5)  # Aguarda antes de tentar novamente

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
