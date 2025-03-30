import os, time
import requests
from bs4 import BeautifulSoup



url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
response = requests.get(url)



if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a', href=True)


    years = [year for year in range(2020,2025)]
    months = ['0' + str(month) if month < 10 else str(month) for month in range(1, 13)]
    taxi_types = {
        'fhv_tripdata':'forHireVehicle',
        'green_tripdata':'greenTaxi',
        'yellow_tripdata':'yellowTaxi',
        'fhvhv_tripdata':'highVolumeForHire',
    }
    script_dir = os.path.dirname(os.path.abspath(__file__)) # Diretório do próprio script
    for taxi_type in taxi_types:
        for year in years:
            for month in months:
                year = str(year)
                month = str(month)
                
                filename = f"{taxi_type}_{year}-{month}.parquet"
                
                link = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"

                dirpath = os.path.join(script_dir, 'tlc_data', taxi_types.get(taxi_type), year) # Diretório onde deverá ser salvo o arquivo
                filepath = os.path.join(dirpath, filename) # diretório + nome do arquivo


                if os.path.exists(dirpath) and filename in os.listdir(dirpath):
                    print(f'O arquivo {filename} já existe no diretório {dirpath}')
                    
                else:
                    if not os.path.exists(dirpath):
                        print(f'O diretório {dirpath} não existe e será criado...')
                        try:
                            os.makedirs(dirpath)
                            print('Diretório criado com sucesso!')
                        except Exception as e:
                            print(f'Erro ao criar o diretório {dirpath}. Exceção:\n\n{e}')



                    print(f'Baixando o arquivo {filename}...')
                    try:
                        file_response = requests.get(link, stream=True)
                        file_response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
                        
                        with open(filepath, 'wb') as f:
                            for chunk in file_response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                        print(f'Arquivo {filename} baixado com sucesso!')
                    except requests.exceptions.RequestException as e:
                        print(f'Erro ao baixar o arquivo {filename}: {e}')
                        

                    time.sleep(5)