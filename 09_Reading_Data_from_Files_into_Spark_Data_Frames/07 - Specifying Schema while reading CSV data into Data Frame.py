# Databricks notebook source
help(spark.read.schema)

# COMMAND ----------

schema = """
    order_id INT,
    order_date TIMESTAMP,
    order_customer_id INT,
    order_status STRING
  """

# COMMAND ----------

# MAGIC %md
# MAGIC - Mehod 1

# COMMAND ----------

spark.read.schema(schema).csv('/public/retail_db/orders').show()

# COMMAND ----------

# MAGIC %md
# MAGIC - Method 2

# COMMAND ----------

spark.read.csv('/public/retail_db/orders', schema = schema).show()

# COMMAND ----------

help(spark.read.format('csv').load)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

schema = StructType([
    StructField('order_id', IntegerType()),
    StructField('order_date', TimestampType()),
    StructField('order_customer_id', IntegerType()),
    StructField('order_status', StringType())
])

# COMMAND ----------

spark.read.schema(schema).csv('/public/retail_db/orders').show()

# COMMAND ----------

spark.read.csv('/public/retail_db/orders', schema = schema).show()

# COMMAND ----------

schema = StructType([
    StructField('order_id', IntegerType(), nullable = False),
    StructField('order_date', TimestampType(), nullable = False),
    StructField('order_customer_id', IntegerType(), nullable = False),
    StructField('order_status', StringType(), nullable = False)
])

# COMMAND ----------

spark.read.schema(schema).csv('/public/retail_db/orders').show()

# COMMAND ----------

spark.read.csv('/public/retail_db/orders', schema = schema).show()

# COMMAND ----------

spark.read.schema(schema).csv('/public/retail_db/orders').printSchema()

# COMMAND ----------


