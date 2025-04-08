```
aws emr add-steps \
  --cluster-id j-2BF29XPYP368K \
  --region us-east-1 \
  --steps Type=Spark,Name="Load to DW and RDS",ActionOnFailure=CONTINUE,\
Args=[--deploy-mode,cluster,--master,yarn,s3://mba-nyc-dataset/emr/scripts/load_to_dw_and_rds.py]
```

--deploy-mode cluster: executa o código diretamente nos nós do EMR.

--master yarn: usa o gerenciador de cluster padrão do EMR (YARN).

O caminho final aponta para seu script .py no S3.
![image](https://github.com/user-attachments/assets/2378cd2e-cc2a-4906-a548-cac07f54d30b)
