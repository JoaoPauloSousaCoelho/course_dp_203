# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

storage_account_name = 'datalakelpbc'
storage_account_access_key = '8DINO/2cl2Dp628ogP4Aop4fABdz6F8ps93eM6yq67ib58Ex3nG6TC6kWHuCoDsONymL8Ug5cS1r+AStTuHK4A=='
spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)

# COMMAND ----------

blob_container = 'raw'
filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/Log.csv"
df = spark.read.format("csv").load(filePath, inferSchema = True, header = True)

# COMMAND ----------

https://datalakelpbc.blob.core.windows.net/raw/Log.csv

# COMMAND ----------

df = spark
