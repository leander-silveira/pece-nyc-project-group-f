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
  --name "EMR PySpark Cluster" \
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

<img width="1394" alt="image" src="https://github.com/user-attachments/assets/493b0d33-750e-49c8-9463-506594890808" />

