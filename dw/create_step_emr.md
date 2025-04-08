```
aws emr add-steps \
  --cluster-id j-2BF29XPYP368K \
  --region us-east-1 \
  --steps Type=Spark,Name="Load to DW and RDS",ActionOnFailure=CONTINUE,\
Args=[--deploy-mode,cluster,--master,yarn,s3://mba-nyc-dataset/emr/scripts/load_to_dw_and_rds.py]
```
