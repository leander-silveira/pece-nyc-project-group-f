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

![image](https://github.com/user-attachments/assets/6b692444-234c-4e94-8f63-da4e685aba52)

4. Verificar status

```
aws rds describe-db-instances \
  --db-instance-identifier nyc-dw-mysql \
  --query "DBInstances[0].DBInstanceStatus"
```
![image](https://github.com/user-attachments/assets/f4d61fef-e45f-4cd6-814b-35c318081bb0)

5. Pegar o endpoint

```
aws rds describe-db-instances \
  --db-instance-identifier nyc-dw-mysql \
  --query "DBInstances[0].Endpoint.Address" \
  --output text
```
