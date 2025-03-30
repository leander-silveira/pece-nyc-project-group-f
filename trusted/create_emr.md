Cria chave :

```
aws ec2 create-key-pair \
  --key-name emr-keypair \
  --key-type rsa \
  --query 'KeyMaterial' \
  --output text > emr-keypair.pem

```
Mudar permissão:
```chmod 400 emr-keypair.pem```

Pegar chave:
```cat emr-keypair.pem```


![image](https://github.com/user-attachments/assets/7c6c790d-9ffc-452d-b376-e9adfcd31af4)

Cria EMR:

```
aws emr create-cluster \
  --name "EMR Trusted Transform" \
  --release-label emr-6.10.0 \
  --applications Name=Spark \
  --ec2-attributes KeyName=emr-keypair \
  --instance-type m5.xlarge \
  --instance-count 3 \
  --use-default-roles \
  --log-uri s3://mba-nyc-dataset/emr/logs/ \
  --steps Type=Spark,Name="Trusted Transform",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,s3://mba-nyc-dataset/emr/scripts/trusted_transform.py] \
  --region us-east-1
 ```
![image](https://github.com/user-attachments/assets/ceeb0427-f4f5-4d46-9e22-aafd26affde1)


![image](https://github.com/user-attachments/assets/e8be5147-5a9d-4cdc-aae3-0caf4eeb2bb4)


Criar um step manualmente
Tentativa 1:
```
aws emr add-steps \
  --cluster-id j-1QE4KP239A1VO \
  --steps Type=Spark,Name="Trusted Transform",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,s3://mba-nyc-dataset/emr/scripts/trusted_transform.py] \
  --region us-east-1
```
Tentativa 2:
```
aws emr add-steps \
  --cluster-id j-3J5BXW4H7BLGY \
  --steps '[{
    "Type":"Spark",
    "Name":"NYC Trusted Transform",
    "ActionOnFailure":"CONTINUE",
    "Args":["spark-submit","--deploy-mode","cluster","--master","yarn","--conf","spark.pyspark.python=python3","s3://mba-nyc-dataset/emr/scripts/trusted_transform.py"]
  }]' \
  --region us-east-1
```

Tentativa 3:

```
aws emr add-steps \
  --cluster-id j-3J5BXW4H7BLGY \
  --steps Type=Spark,Name="NYC Trusted Transform",ActionOnFailure=CONTINUE,Args=["--deploy-mode","cluster","--master","yarn","--conf","spark.pyspark.python=python3","s3://mba-nyc-dataset/emr/scripts/trusted_transform.py"] \
  --region us-east-1
```

Tentativa 4:
```
aws emr add-steps \
  --region us-east-1 \
  --cluster-id j-3J5BXW4H7BLGY \
  --steps Type=Spark,Name="NYC Trusted Transform",ActionOnFailure=CONTINUE,Args=[\
"spark-submit",\
"--deploy-mode","cluster",\
"--master","yarn",\
"s3://mba-nyc-dataset/emr/scripts/trusted_transform.py"\
]
```

![image](https://github.com/user-attachments/assets/c87db34f-5964-4b0f-8b75-67fb3a6b3f26)

![image](https://github.com/user-attachments/assets/defa0c06-8eda-42ea-8b40-ff68c982d69a)
![image](https://github.com/user-attachments/assets/2eea6a83-36ed-4508-9d61-c96a3c40ee53)

