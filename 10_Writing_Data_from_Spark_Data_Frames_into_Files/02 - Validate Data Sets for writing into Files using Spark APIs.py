# Databricks notebook source
# MAGIC %fs ls /public/retail_db

# COMMAND ----------

# MAGIC %fs ls /public/retail_db/orders

# COMMAND ----------

orders = spark. \
        read.\
        csv('/public/retail_db/orders', inferSchema=True).\
        toDF('order_id','order_date','order_customer_id','order_status')

# COMMAND ----------

orders.inputFiles()

# COMMAND ----------

orders.dtypes

# COMMAND ----------

orders.show()

# COMMAND ----------

order_items = spark.read.json('/public/retail_db_json/order_items')

# COMMAND ----------

order_items.inputFiles()

# COMMAND ----------

order_items.show()

# COMMAND ----------

import getpass

# COMMAND ----------

username = getpass.getuser()

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

df_parquet = spark.read.parquet(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

df_parquet.inputFiles()

# COMMAND ----------

df_parquet.show()

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/retail_db_pipe/orders')

# COMMAND ----------

spark.read.csv(f'/user/{username}/retail_db_pipe/orders', sep = '|')

# COMMAND ----------


