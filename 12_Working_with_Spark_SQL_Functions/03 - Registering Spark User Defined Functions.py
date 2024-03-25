# Databricks notebook source
# MAGIC %md
# MAGIC Here are the steps we need to follow to develop and use Spark UDFs
# MAGIC - Develop required logic using Python as programming language
# MAGIC - Register the function using `spark.udf.register` . Also assign it to a variable
# MAGIC - Variable can be used as part of Data Frame APIs such as `select`, `filter` etc
# MAGIC - When we register, we register with a name. That name can be used as part of `selectExpr` or as part of Spark SQL queries using spark.sql

# COMMAND ----------

help(spark.udf.register)

# COMMAND ----------

df = spark.read.json('/public/retail_db_json/orders')

# COMMAND ----------

df.show()

# COMMAND ----------

dc = spark.udf.register('date_convert', lambda d: int(d[:10].replace('-','')))

# COMMAND ----------

dc

# COMMAND ----------

df.select(dc('order_date').alias('order_date')).show()

# COMMAND ----------

df.filter(dc('order_date') == 20140101).show()

# COMMAND ----------

df. \
    groupBy(dc('order_date').alias('order_date')).\
    count().\
    withColumnRenamed('count','order_count').\
    show()

# COMMAND ----------


