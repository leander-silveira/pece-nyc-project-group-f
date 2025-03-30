1. Criar uma Chave SSH
`aws ec2 create-key-pair --key-name ChaveEC2 --query 'KeyMaterial' --output text > ChaveEC2.pem`
<img width="550" alt="image" src="https://github.com/user-attachments/assets/823bd89c-07ca-4671-b3c6-8b4e8a007db1" />

Pegar valor da chave:
`cat ChaveEC2.pem`
2. Escolher a Imagem (AMI) e o Tipo de Instância
`aws ec2 describe-images --owners amazon --filters "Name=name,Values=amzn2-ami-hvm-*" --query 'Images[*].[ImageId, Name]' --output table`


<img width="781" alt="image" src="https://github.com/user-attachments/assets/e488c1b6-6287-481d-9eae-ea16f8f155be" />

Usar a mais recente do tipo x86_64-ebs, como:

AMI ID: ami-052b9fbb6949f883a
Nome: amzn2-ami-hvm-2.0.20240916.0-x86_64-ebs

3. Criar um Security Group

```
SECURITY_GROUP_ID=$(aws ec2 create-security-group --group-name MeuSecurityGroup --description "Acesso SSH" --query 'GroupId' --output text)

aws ec2 authorize-security-group-ingress --group-id $SECURITY_GROUP_ID --protocol tcp --port 22 --cidr 0.0.0.0/0
```

<img width="798" alt="image" src="https://github.com/user-attachments/assets/d821e6c5-778a-4c02-be26-abf8dced0489" />


4. Criar a Instância
```
INSTANCE_ID=$(aws ec2 run-instances \
  --image-id ami-052b9fbb6949f883a \
  --count 1 \
  --instance-type t2.large \
  --key-name ChaveEC2 \
  --security-group-ids $SECURITY_GROUP_ID \
  --block-device-mappings '[{"DeviceName": "/dev/xvda","Ebs":{"VolumeSize":100}}]' \
  --query 'Instances[0].InstanceId' \
  --output text)
```

<img width="486" alt="image" src="https://github.com/user-attachments/assets/a71a22ad-f579-4df4-a48e-15f53483e9b3" />

5. Obter o IP da Instância
`aws ec2 describe-instances --instance-ids $INSTANCE_ID --query 'Reservations[0].Instances[0].PublicIpAddress' --output text`
54.80.44.36
<img width="707" alt="image" src="https://github.com/user-attachments/assets/54c63e95-cdbd-4e5d-bae5-fdb9725df3a3" />

6.Copiar o Arquivo do S3 para a EC2

`aws s3 cp s3://mba-nyc-dataset/web_scraping.py ~/web_scraping.py`

`scp -i ~/.ssh/cloud9-key.pem s3://mba-nyc-dataset/web_scraping.py ec2-user@54.80.44.36:~/`

<img width="396" alt="image" src="https://github.com/user-attachments/assets/62253182-9f23-48ff-ad52-3da1062d55bc" />

7. Conectar via SSH

`ssh -i ChaveEC2.pem ec2-user@54.80.44.36`

<img width="451" alt="image" src="https://github.com/user-attachments/assets/33682aec-05d0-4114-96c8-571f8b85fe6f" />

8.Instalação Python

<img width="444" alt="image" src="https://github.com/user-attachments/assets/4731de72-5e0f-46b9-aaad-26eff331b02c" />
<img width="288" alt="image" src="https://github.com/user-attachments/assets/9a3b6b04-734f-474b-bf80-2698ebadc403" />


Chave usada pra criar instância EC2

`aws ec2 describe-instances --instance-ids i-08741825f813f9b01 --query 'Reservations[0].Instances[0].KeyName' --region us-east-1`

<img width="855" alt="image" src="https://github.com/user-attachments/assets/b5df29a8-993e-4cc5-b3aa-5657a44c76f6" />


