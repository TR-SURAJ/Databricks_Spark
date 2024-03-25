# Databricks notebook source
order_items = spark.read.json('/public/retail_db_json/order_items')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - Get number of records in order_item

# COMMAND ----------

order_items.count()

# COMMAND ----------

type(order_items.count())

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

# count is wide transformation. It will be triggered when we perform actions such as show
order_items.select(count("*"))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


