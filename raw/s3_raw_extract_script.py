import os, time
import requests
from bs4 import BeautifulSoup
import boto3
import re

url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
response = requests.get(url)

s3 = boto3.client("s3")
S3_BUCKET = "mba-nyc-dataset"
S3_PREFIX = "raw"

if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a', href=True)

    # Coletar todos os links de arquivos parquet válidos
    file_links = [link['href'] for link in links if link['href'].endswith('.parquet') and 'trip-data' in link['href']]

    taxi_types = {
        'fhv_tripdata': 'forHireVehicle',
        'green_tripdata': 'greenTaxi',
        'yellow_tripdata': 'yellowTaxi',
        'fhvhv_tripdata': 'highVolumeForHire',
    }

    # Regex para extrair tipo, ano e mês do nome do arquivo
    pattern = re.compile(r'(fhv_tripdata|green_tripdata|yellow_tripdata|fhvhv_tripdata)_(\d{4})-(\d{2})\.parquet')

    for link in file_links:
        match = pattern.search(link)
        if match:
            taxi_type, year, month = match.groups()
            taxi_name = taxi_types.get(taxi_type)

            filename = f"{taxi_type}_{year}-{month}.parquet"
            s3_key = f"{S3_PREFIX}/{taxi_name}/{year}/{filename}"
            full_link = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"

            print(f'⤵️ Baixando o arquivo {filename}...')
            try:
                file_response = requests.get(full_link, stream=True)
                file_response.raise_for_status()

                print(f'⤴️ Fazendo upload de {filename} para s3://{S3_BUCKET}/{s3_key}...')
                s3.upload_fileobj(file_response.raw, S3_BUCKET, s3_key)
                print(f'✅ Arquivo {filename} enviado para o S3 com sucesso!\n')
            except requests.exceptions.RequestException as e:
                print(f'❌ Erro ao baixar o arquivo {filename}: {e}')
            except Exception as e:
                print(f'❌ Erro ao enviar o arquivo {filename} para o S3: {e}')
            
            print('=======================================================================\n')
            time.sleep(3)
