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



Cria EMR:
```
aws emr create-cluster \
  --name "EMR Trusted Layer Transform" \
  --release-label emr-6.10.0 \
  --applications Name=Spark \
  --ec2-attributes KeyName=emr-keypair \
  --instance-type m5.xlarge \
  --instance-count 3 \
  --use-default-roles \
  --log-uri s3://mba-nyc-dataset/emr/logs/ \
  --steps Type=Spark,Name="Trusted Transform",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,s3://mba-nyc-dataset/scripts/trusted_transform.py] \
  --auto-terminate \
  --region us-east-1
```


![image](https://github.com/user-attachments/assets/2b2b7229-cec8-4ad8-8bba-1b4346404366)

<img width="1416" alt="image" src="https://github.com/user-attachments/assets/2251afed-10c2-4fb7-8009-8a943a139c05" />


Obter o DNS público da instância mestre
```aws emr describe-cluster --cluster-id j-2YHNZ217IHWV8 --query 'Cluster.MasterPublicDnsName' --output text```


<img width="634" alt="image" src="https://github.com/user-attachments/assets/58c0a5fb-3235-4f0a-8a43-b723584b0294" />

Criar túnel para o Jupyter Notebook
```ssh -i emr-keypair.pem -N -L 8888:localhost:8888 hadoop@ec2-54-164-19-230.compute-1.amazonaws.com```


<img width="577" alt="image" src="https://github.com/user-attachments/assets/0bbe3cf5-d682-492e-8810-1c0b2507fee4" />

Acessar:

```http://localhost:8888```

