# Databricks notebook source
df = spark.read.json('/public/retail_db_json/orders')

# COMMAND ----------

df.dtypes

# COMMAND ----------

dc = spark.udf.register('date_convert', lambda d:d[:10].replace('-',''))

# COMMAND ----------

df.selectExpr('date_convert(order_date) AS order_date').show()

# COMMAND ----------

df.createOrReplaceTempView('orders')

# COMMAND ----------

spark.sql('''
    SELECT o.*, date_convert(order_date) AS order_date_as_int
    FROM orders AS o
 ''').show()

# COMMAND ----------

spark.sql('''
    SELECT o.*, date_convert(order_date) AS order_date_as_int
    FROM orders AS o
    WHERE date_convert(order_date) = 20140101
 ''').show()

# COMMAND ----------

spark.sql('''
    SELECT date_convert(order_date) AS order_date, count(*) AS order_count
    FROM orders
    GROUP BY 1
 ''').show()

# COMMAND ----------


