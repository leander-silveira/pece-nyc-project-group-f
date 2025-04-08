1. Criar um grupo de segurança para o RDS

```
aws ec2 create-security-group \
  --group-name nyc-mysql-sg \
  --description "SG para RDS MySQL DW NYC" \
  --vpc-id $(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query "Vpcs[0].VpcId" --output text)
```

2. Liberar acesso à porta MySQL (3306)

```
aws ec2 authorize-security-group-ingress \
  --group-name nyc-mysql-sg \
  --protocol tcp \
  --port 3306 \
  --cidr 0.0.0.0/0
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
  --vpc-security-group-ids <SEU_SECURITY_GROUP_ID> \
  --availability-zone us-east-1a \
  --publicly-accessible \
  --backup-retention-period 0 \
  --no-multi-az

```

4. Verificar status

```
aws rds describe-db-instances \
  --db-instance-identifier nyc-dw-mysql \
  --query "DBInstances[0].DBInstanceStatus"
```

5. Pegar o endpoint

```
aws rds describe-db-instances \
  --db-instance-identifier nyc-dw-mysql \
  --query "DBInstances[0].Endpoint.Address" \
  --output text
```
