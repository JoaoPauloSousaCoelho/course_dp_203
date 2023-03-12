# Databricks notebook source
# MAGIC %fs ls 
# MAGIC dbfs:/FileStore/tables/

# COMMAND ----------

df = spark.read.option('header', 'true').option('inferSchema', 'true').csv('dbfs:/FileStore/tables/Log.csv')

# COMMAND ----------

display(df)

# COMMAND ----------

df.select('Id', 'Status').show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df.filter('Status = "Succeeded"'))

# COMMAND ----------

display(df.groupBy('Status').count())

# COMMAND ----------

from pyspark.sql.functions import month, year, dayofyear, col, to_date
display(df.select(year(col("time")).alias('Year'), month(col("time")).alias('Month'), to_date(col('time'), 'dd-mm-yyy').alias('Date')))

# COMMAND ----------

display(df.filter(col('Resourcegroup').isNotNull()))

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

schema = StructType([
    StructField("Data", StringType(), True),
    StructField("Temp", IntegerType(), True)
])

# COMMAND ----------

dados = [{"Data": "2021-01-18", "Temp": 3}, {"Data": "2021-01-19", "Temp": 4}, {"Data":"2021-01-20", "Temp": 2}, {"Data": "2021-01-21", "Temp": 2}]


# COMMAND ----------

rdd = sc.parallelize(dados)
df = spark.createDataFrame(rdd, schema)
df.show()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable('Temperatures')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT YEAR(Data) Year, MONTH(Data) Month, Temp
# MAGIC FROM temperatures
# MAGIC WHERE Data BETWEEN Date '2019-01-01' AND DATE '2021-12-31')
# MAGIC PIVOT(AVG(CAST(Temp AS FLOAT))
# MAGIC FOR Month in 
# MAGIC (1 JAN, 2 FEB, 3 MAR, 4 APR, 5 MAY, 6 JUN, 7 JUL, 8 AUG, 9 SEP, 10 OCT, 11 NOV, 12 DEC)) ORDER BY YEAR ASC
