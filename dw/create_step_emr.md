```
aws emr add-steps \
  --cluster-id j-2BF29XPYP368K \
  --steps Type=Spark,Name="Load Trusted to DW and RDS",ActionOnFailure=CONTINUE,\
Args=[--deploy-mode,cluster,\
--master,yarn,\
--jars,s3://mba-nyc-dataset/emr/jars/mysql-connector-j-8.0.33.jar,\
s3://mba-nyc-dataset/emr/scripts/load_to_dw_and_rds.py]
```

--deploy-mode cluster: executa o código diretamente nos nós do EMR.

--master yarn: usa o gerenciador de cluster padrão do EMR (YARN).

O caminho final aponta para seu script .py no S3.
![image](https://github.com/user-attachments/assets/2378cd2e-cc2a-4906-a548-cac07f54d30b)
