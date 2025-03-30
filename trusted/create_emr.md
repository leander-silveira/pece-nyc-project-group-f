Cria chave :

```
aws ec2 create-key-pair \
  --key-name emr-keypair \
  --key-type rsa \
  --query 'KeyMaterial' \
  --output text > emr-keypair.pem

```

Cria EMR:
```
aws emr create-cluster \
  --name "EMR PySpark Cluster" \
  --release-label emr-6.10.0 \
  --applications Name=JupyterEnterpriseGateway Name=Spark \
  --ec2-attributes KeyName=seu-keypair,InstanceProfile=EMR_EC2_DefaultRole \
  --service-role EMR_DefaultRole \
  --instance-type m5.xlarge \
  --instance-count 3 \
  --use-default-roles \
  --log-uri s3://<seu-bucket-de-logs-emr>/logs/ \
  --bootstrap-actions Path="s3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/install-jupyter-emr6.sh" \
  --configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"}}]' \
  --region us-east-1 \
  --auto-terminate
```
