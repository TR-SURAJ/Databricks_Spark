# Databricks notebook source
orders = spark.read.json('/public/retail_db_json/orders')

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

help(count)

# COMMAND ----------

orders.show()

# COMMAND ----------

orders.groupBy('order_status').agg(count('*')).show()

# COMMAND ----------


