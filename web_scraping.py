import requests
from bs4 import BeautifulSoup
import boto3
from tqdm import tqdm
from io import BytesIO

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
    links = []

    # Procura links para arquivos CSV e Parquet
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "tripdata" in href and (href.endswith(".parquet") or href.endswith(".csv")):
            full_url = href if href.startswith("http") else f"https://www.nyc.gov{href}"
            links.append(full_url)

    print(f"✅ {len(links)} links encontrados!")
    return links

# Função para baixar e enviar o arquivo direto para o S3
def download_and_upload_to_s3(url):
    filename = url.split("/")[-1]
    s3_key = f"{S3_PREFIX}/{filename}"

    # Verifica se o arquivo já existe no S3
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        print(f"⏩ {s3_key} já existe no S3, pulando download.")
        return
    except:
        pass  # Continua para fazer o download

    print(f"⬇ Baixando {filename}...")

    response = requests.get(url, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    progress_bar = tqdm(total=total_size, unit="B", unit_scale=True, unit_divisor=1024, desc=filename)

    # Salva os dados diretamente na memória (sem armazenar no disco)
    file_data = BytesIO()
    for chunk in response.iter_content(chunk_size=1024):
        file_data.write(chunk)
        progress_bar.update(len(chunk))
    progress_bar.close()

    file_data.seek(0)  # Volta para o início do arquivo

    print(f"🚀 Enviando {filename} para s3://{S3_BUCKET}/{s3_key}...")
    s3.upload_fileobj(file_data, S3_BUCKET, s3_key)
    print(f"✅ Upload concluído: s3://{S3_BUCKET}/{s3_key}")

# Baixa e envia os arquivos para o S3
def main():
    links = get_download_links()
    for link in links:
        try:
            download_and_upload_to_s3(link)
        except Exception as e:
            print(f"❌ Erro ao processar {link}: {e}")

if __name__ == "__main__":
    main()
