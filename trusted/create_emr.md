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

"ClusterId": "j-1QE4KP239A1VO",

![image](https://github.com/user-attachments/assets/ac38642f-7c0f-4ed9-b301-53be9b3283ea)

![image](https://github.com/user-attachments/assets/907f555a-3fb4-450e-9b5b-1f27e95afb1b)

![image](https://github.com/user-attachments/assets/7c6c790d-9ffc-452d-b376-e9adfcd31af4)


Criar um step manualmente

aws emr add-steps \
  --cluster-id j-1QE4KP239A1VO \
  --steps Type=Spark,Name="Trusted Transform",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,s3://mba-nyc-dataset/emr/scripts/trusted_transform.py] \
  --region us-east-1



