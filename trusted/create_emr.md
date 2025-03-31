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
  --name "EMR PySpark Cluster" \
  --log-uri "s3://mba-nyc-dataset/emr/logs" \
  --release-label "emr-6.10.0" \
  --service-role "EMR_DefaultRole" \
  --unhealthy-node-replacement \
  --ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole","EmrManagedMasterSecurityGroup":"sg-039e4551e594b0810","EmrManagedSlaveSecurityGroup":"sg-0ecce9d7903e4424f","KeyName":"emr-keypair","AvailabilityZone":"us-east-1a"}' \
  --applications Name=Spark \
  --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","Name":"MASTER","InstanceType":"m5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}},{"InstanceCount":2,"InstanceGroupType":"CORE","Name":"CORE","InstanceType":"m5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}}]' \
  --steps '[{"Name":"NYC Trusted Transform","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Args":["spark-submit","--deploy-mode","cluster","--master","yarn","--conf","spark.pyspark.python=python3","s3://mba-nyc-dataset/emr/scripts/trusted_transform.py"],"Type":"CUSTOM_JAR"}]' \
  --scale-down-behavior "TERMINATE_AT_TASK_COMPLETION" \
  --region "us-east-1"
```

![image](https://github.com/user-attachments/assets/4b1329b3-baf5-43d4-9ad4-f3542a037f3b)
![image](https://github.com/user-attachments/assets/dbf60f45-74a3-4ce4-896e-5a084fa39aa8)

![image](https://github.com/user-attachments/assets/872a2a31-31d6-430a-a9a2-4c5711824d94)


Criar um step manualmente

```
aws emr add-steps \
  --region us-east-1 \
  --cluster-id j-263R8D2GGP9I3 \
  --steps '[ 
    {
      "Name": "NYC Trusted Transform - Atualizado",
      "ActionOnFailure": "CONTINUE",
      "Type": "CUSTOM_JAR",
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode", "cluster",
        "--master", "yarn",
        "--conf", "spark.pyspark.python=python3",
        "s3://mba-nyc-dataset/emr/scripts/trusted_transform.py"
      ]
    }
  ]'

```
![image](https://github.com/user-attachments/assets/291f7d4a-ff7a-4161-99e9-bb183d06fa04)

