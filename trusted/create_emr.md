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
 --log-uri "s3://mba-nyc-dataset/emr/logs" \
 --release-label "emr-6.10.0" \
 --service-role "EMR_DefaultRole" \
 --auto-terminate \
 --unhealthy-node-replacement \
 --ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole","EmrManagedMasterSecurityGroup":"sg-039e4551e594b0810","EmrManagedSlaveSecurityGroup":"sg-0ecce9d7903e4424f","KeyName":"emr-keypair","AvailabilityZone":"us-east-1a"}' \
 --applications Name=Spark \
 --instance-groups '[{"InstanceCount":2,"InstanceGroupType":"CORE","Name":"CORE","InstanceType":"m5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}},{"InstanceCount":1,"InstanceGroupType":"MASTER","Name":"MASTER","InstanceType":"m5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}}]' \
 --scale-down-behavior "TERMINATE_AT_TASK_COMPLETION" \
 --region "us-east-1"
 ```


![image](https://github.com/user-attachments/assets/2b2b7229-cec8-4ad8-8bba-1b4346404366)

![image](https://github.com/user-attachments/assets/69b88428-a57c-47ce-a352-ff816deabfcb)

![image](https://github.com/user-attachments/assets/775fe085-1c51-4f67-9cdb-faf11e738909)




