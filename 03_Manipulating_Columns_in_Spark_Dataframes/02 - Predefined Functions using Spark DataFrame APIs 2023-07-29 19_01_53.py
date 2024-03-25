# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC * We typically process data in columns using functions in **pyspark.sql.functions** . Let us understand details about these functions in detail as part of this module

# COMMAND ----------

orders = spark.read.csv('/public/retail_db/orders', schema = 'order_id INT, order_date STRING, order_customer_id INT, order_status STRING')

# COMMAND ----------

orders.show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

help(date_format)

# COMMAND ----------

orders.select('*', date_format('order_date','yyyyMM').alias('order_month')).show()

# COMMAND ----------

orders.withColumn('order_month', date_format('order_date', 'yyyyMM')).show()

# COMMAND ----------

# where or filter

orders.filter(date_format('order_date','yyyyMM') == 201401).show()

# COMMAND ----------

orders.groupBy(date_format('order_date','yyyyMM').alias('order_month')).count().show()

# COMMAND ----------


