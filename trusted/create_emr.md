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

Tentativa 1

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


Tentativa 2:

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
![image](https://github.com/user-attachments/assets/c87db34f-5964-4b0f-8b75-67fb3a6b3f26)

![image](https://github.com/user-attachments/assets/defa0c06-8eda-42ea-8b40-ff68c982d69a)
![image](https://github.com/user-attachments/assets/2eea6a83-36ed-4508-9d61-c96a3c40ee53)

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


![image](https://github.com/user-attachments/assets/83458967-3bd0-4a5d-9fff-6dcc48c22c06)

Tentativa 5:
```
aws emr add-steps \
  --region us-east-1 \
  --cluster-id j-3J5BXW4H7BLGY \
  --steps '[
    {
      "Type": "Spark",
      "Name": "NYC Trusted Transform",
      "ActionOnFailure": "CONTINUE",
      "Args": [
        "spark-submit",
        "--deploy-mode", "cluster",
        "--master", "yarn",
        "s3://mba-nyc-dataset/emr/scripts/trusted_transform.py"
      ]
    }
  ]'
```
![image](https://github.com/user-attachments/assets/5205d6c1-77dd-40ad-8145-7a3cb6884a24)


Tentativa 6:

```
aws emr add-steps \
  --region us-east-1 \
  --cluster-id j-3J5BXW4H7BLGY \
  --steps '[
    {
      "Type": "Spark",
      "Name": "NYC Trusted Transform",
      "ActionOnFailure": "CONTINUE",
      "Args": [
        "--deploy-mode", "cluster",
        "--master", "yarn",
        "--conf", "spark.pyspark.python=python3",
        "s3://mba-nyc-dataset/emr/scripts/trusted_transform.py"
      ]
    }
  ]'
```
![image](https://github.com/user-attachments/assets/b034334a-89a6-4e30-a90e-3f12a1df2553)
