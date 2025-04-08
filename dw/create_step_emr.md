```
aws emr add-steps \
  --cluster-id j-2BF29XPYP368K \
  --steps 'Type=Spark,Name="Load To DW and RDS",ActionOnFailure=CONTINUE,Args=["spark-submit","--deploy-mode","cluster","--master","yarn","--conf","spark.jars=s3://mba-nyc-dataset/emr/jars/mysql-connector-j-8.0.33.jar","s3://mba-nyc-dataset/emr/scripts/load_to_dw_and_rds.py"]'

```

![image](https://github.com/user-attachments/assets/a03a2d3b-bf9b-4443-bce2-60bb2f15b8a0)

`--deploy-mode cluster`: roda o script diretamente no cluster EMR.

`--master yarn`: usa o YARN como gerenciador de recursos.

`--jars`: adiciona o conector JDBC necessário para escrever no RDS MySQL.

`s3://.../load_to_dw_and_rds.py`: caminho script no S3.
