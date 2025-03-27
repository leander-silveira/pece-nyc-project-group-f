import boto3
import datetime


def trusted_transform(s3, month, year, taxi_type_folder, taxi_type_filename):
    filename = f"{taxi_type_filename}_{year}-{month}.parquet"
    path_filename = f"{taxi_type_folder}/{year}/{filename}"
    bucket = "mba-nyc-dataset-f"
    source_key = f"raw/{path_filename}"
    destination_key = f"trusted/{path_filename}"
    
    copy_source = {"Bucket":bucket, "Key":source_key}
    try:
        s3.copy_object(CopySource=copy_source, Bucket=bucket, Key=destination_key)
        print(f'Arquivo {filename} trasnferido com sucesso para a pasta {taxi_type_folder}')
    except Exception as e:
        print(f'Falha ao transferir o arquivo {filename}  para a pasta {taxi_type_folder}')
        
### Função para ler arquivo


taxi_types = {
    'fhv_tripdata': 'forHireVehicle',
    'green_tripdata': 'greenTaxi',
    'yellow_tripdata': 'yellowTaxi',
    'fhvhv_tripdata': 'highVolumeForHire',
}
today = datetime.datetime.now()
today_year = today.year
today_month = today.month

years = [year for year in range(2022, today_year+1)]
months = ['0' + str(month) if month < 10 else str(month) for month in range(1, 13)]

s3 = boto3.client("s3")

for year in years:
    for month in months:
        for taxi_type in taxi_types:
            taxi_type_folder = taxi_types.get(taxi_type)
            taxi_type_filename = taxi_type
            
            # Calling function to load
            trusted_transform(
                s3=s3,
                month=month,
                year=year,
                taxi_type_folder=taxi_type_folder,
                taxi_type_filename=taxi_type_filename
            )
            

### Try a new structure using year/month/files.parquet