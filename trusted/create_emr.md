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
  --name "EMR PySpark" \
  --release-label emr-6.10.0 \
  --applications Name=JupyterEnterpriseGateway Name=Spark \
  --ec2-attributes KeyName=emr-keypair \
  --instance-type m5.xlarge \
  --instance-count 3 \
  --use-default-roles \
  --log-uri s3://mba-nyc-dataset/emr/logs/ \
  --bootstrap-actions Path="s3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/install-jupyter-emr6.sh" \
  --configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"}}]' \
  --region us-east-1
```

<img width="618" alt="image" src="https://github.com/user-attachments/assets/0cf6316d-4b46-41fc-a9cd-c15105550ecc" />

<img width="1416" alt="image" src="https://github.com/user-attachments/assets/2251afed-10c2-4fb7-8009-8a943a139c05" />


Obter o DNS público da instância mestre
```aws emr describe-cluster --cluster-id j-2YHNZ217IHWV8 --query 'Cluster.MasterPublicDnsName' --output text```


<img width="634" alt="image" src="https://github.com/user-attachments/assets/58c0a5fb-3235-4f0a-8a43-b723584b0294" />

Criar túnel para o Jupyter Notebook
```ssh -i emr-keypair.pem -N -L 8888:localhost:8888 hadoop@ec2-54-164-19-230.compute-1.amazonaws.com```


<img width="577" alt="image" src="https://github.com/user-attachments/assets/0bbe3cf5-d682-492e-8810-1c0b2507fee4" />

Acessar:

```http://localhost:8888```

