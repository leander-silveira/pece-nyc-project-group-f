```
aws emr add-steps \
  --cluster-id j-2BF29XPYP368K \
  --steps Type=Spark,Name="Load Trusted to DW and RDS",ActionOnFailure=CONTINUE,\
Args=[--deploy-mode,cluster,\
--master,yarn,\
--jars,s3://mba-nyc-dataset/emr/jars/mysql-connector-j-8.0.33.jar,\
s3://mba-nyc-dataset/emr/scripts/load_to_dw_and_rds.py]
```

![image](https://github.com/user-attachments/assets/6b8a4e23-f02d-4682-8c0c-fb524cc6726b)

`--deploy-mode cluster`: roda o script diretamente no cluster EMR.

`--master yarn`: usa o YARN como gerenciador de recursos.

`--jars`: adiciona o conector JDBC necessário para escrever no RDS MySQL.

`s3://.../load_to_dw_and_rds.py`: caminho script no S3.
