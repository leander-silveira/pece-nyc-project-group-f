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
{
    "ClusterId": "j-27UZJUZTRD7TF",
    "ClusterArn": "arn:aws:elasticmapreduce:us-east-1:178179121271:cluster/j-27UZJUZTRD7TF"
}

![image](https://github.com/user-attachments/assets/ac38642f-7c0f-4ed9-b301-53be9b3283ea)

![image](https://github.com/user-attachments/assets/907f555a-3fb4-450e-9b5b-1f27e95afb1b)

![image](https://github.com/user-attachments/assets/7c6c790d-9ffc-452d-b376-e9adfcd31af4)


Criar um step manualmente

```
aws emr add-steps \
  --cluster-id j-1QE4KP239A1VO \
  --steps Type=Spark,Name="Trusted Transform",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,s3://mba-nyc-dataset/emr/scripts/trusted_transform.py] \
  --region us-east-1
```


![image](https://github.com/user-attachments/assets/93109078-f82f-4ea3-bf2d-103ed3d8fa48)

