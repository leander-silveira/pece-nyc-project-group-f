import os, time
import requests
from bs4 import BeautifulSoup
import boto3
import datetime

url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
response = requests.get(url)

s3 = boto3.client("s3")
S3_BUCKET = "mba-nyc-dataset-f"
S3_PREFIX = "raw"

year_today = datetime.datetime.now().year

if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a', href=True)

    years = [year for year in range(year_today, year_today+1)]
    months = ['0' + str(month) if month < 10 else str(month) for month in range(1, 13)]
    
    
    taxi_types = {
        'fhv_tripdata': 'forHireVehicle',
        'green_tripdata': 'greenTaxi',
        'yellow_tripdata': 'yellowTaxi',
        'fhvhv_tripdata': 'highVolumeForHire',
    }

    for taxi_type, taxi_name in taxi_types.items():
        for year in years:
            for month in months:
                year = str(year)
                month = str(month)

                filename = f"{taxi_type}_{year}-{month}.parquet"
                link = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
                s3_key = f"{S3_PREFIX}/{taxi_name}/{year}/{filename}"

                print(f'⤵️ Baixando o arquivo {filename}...')
                try:
                    file_response = requests.get(link, stream=True)
                    file_response.raise_for_status()  # Lança exceção para status de erro (4xx ou 5xx)

                    print(f'⤴️ Fazendo upload de {filename} para s3://{S3_BUCKET}/{s3_key}...')
                    s3.upload_fileobj(file_response.raw, S3_BUCKET, s3_key)
                    print(f'✅ Arquivo {filename} enviado para o S3 com sucesso!\n')
                except requests.exceptions.RequestException as e:
                    print(f'❌ Erro ao baixar o arquivo {filename}: {e}')
                except Exception as e:
                    print(f'❌ Erro ao enviar o arquivo {filename} para o S3: {e}')
                
                print('=======================================================================\n')
                time.sleep(3)