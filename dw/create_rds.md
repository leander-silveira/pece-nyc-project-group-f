1. Criar um grupo de segurança para o RDS

```
aws ec2 create-security-group \
  --group-name nyc-mysql-sg \
  --description "SG para RDS MySQL DW NYC" \
  --vpc-id $(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query "Vpcs[0].VpcId" --output text)
```

![image](https://github.com/user-attachments/assets/3d1af60f-546c-459f-b232-20202d48e50a)

Grupo retornado:
```
{
    "GroupId": "sg-0705f0473d9bcdc1b",
    "SecurityGroupArn": "arn:aws:ec2:us-east-1:178179121271:security-group/sg-0705f0473d9bcdc1b"
}
```

2. Liberar acesso à porta MySQL (3306)

```
aws ec2 authorize-security-group-ingress \
  --group-name nyc-mysql-sg \
  --protocol tcp \
  --port 3306 \
  --cidr 0.0.0.0/0
```
![image](https://github.com/user-attachments/assets/cc4729eb-578a-48e8-95de-fcf0949eb807)

Retorno:
```
{
    "Return": true,
    "SecurityGroupRules": [
        {
            "SecurityGroupRuleId": "sgr-00e8b42cd2a6ab08b",
            "GroupId": "sg-0705f0473d9bcdc1b",
            "GroupOwnerId": "178179121271",
            "IsEgress": false,
            "IpProtocol": "tcp",
            "FromPort": 3306,
            "ToPort": 3306,
            "CidrIpv4": "0.0.0.0/0",
            "SecurityGroupRuleArn": "arn:aws:ec2:us-east-1:178179121271:security-group-rule/sgr-00e8b42cd2a6ab08b"
        }
    ]
}
```
3. Criar o RDS MySQL

```
aws rds create-db-instance \
  --db-instance-identifier nyc-dw-mysql \
  --db-instance-class db.t3.micro \
  --engine mysql \
  --allocated-storage 20 \
  --master-username admin \
  --master-user-password SuaSenhaForte123 \
  --vpc-security-group-ids sg-0705f0473d9bcdc1b \
  --availability-zone us-east-1a \
  --publicly-accessible \
  --backup-retention-period 0 \
  --no-multi-az

```
![image](https://github.com/user-attachments/assets/d9c78ba6-eb3d-446d-9b55-caead2d065a6)

![image](https://github.com/user-attachments/assets/682eca06-631e-4dc2-b9de-0245c5409502)

4. Verificar status

```
aws rds describe-db-instances \
  --db-instance-identifier nyc-dw-mysql \
  --query "DBInstances[0].DBInstanceStatus"
```
![image](https://github.com/user-attachments/assets/2c6fcc00-bd4a-45a3-bb84-b19a2af230f4)

5. Pegar o endpoint

```
aws rds describe-db-instances \
  --db-instance-identifier nyc-dw-mysql \
  --query "DBInstances[0].Endpoint.Address" \
  --output text
```
nyc-dw-mysql.coseekllgrql.us-east-1.rds.amazonaws.com

![image](https://github.com/user-attachments/assets/ed51b034-fa6a-4ad3-904a-d08d5c2b9fc2)

6.Pegar usuário e senha
Seção "Configurações":
- Usuário mestre (Master username): admin
- Senha: não aparece no console por segurança

6.1 Redefinir a senha:

- Modificar
- Configurações
- Novo grupo de configurações
- Nova senha mestre
- Continuar
- Modificar instância

A alteração da senha não reinicia a instância

7. Instalar o JDBC do MySQL, e salvar no EMR e Cloud9

https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar


Cloud9:
```
mkdir -p /home/ec2-user/spark_jars
wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar -P /home/ec2-user/spark_jars/
```

EMR:
```s3://mba-nyc-dataset/emr/jars/mysql-connector-j-8.0.33.jar```
![image](https://github.com/user-attachments/assets/d7183673-c428-41af-9c37-bc067c11a23e)

