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


![image](https://github.com/user-attachments/assets/75dcbb36-7b2f-4bd6-8896-6bc704bed207)






Criar um step manualmente

```
aws emr add-steps \
  --cluster-id j-1QE4KP239A1VO \
  --steps Type=Spark,Name="Trusted Transform",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,s3://mba-nyc-dataset/emr/scripts/trusted_transform.py] \
  --region us-east-1
```

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

![image](https://github.com/user-attachments/assets/93109078-f82f-4ea3-bf2d-103ed3d8fa48)

