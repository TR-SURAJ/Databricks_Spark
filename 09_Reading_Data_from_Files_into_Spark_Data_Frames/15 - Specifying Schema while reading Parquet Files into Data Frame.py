# Databricks notebook source
import getpass
username = getpass.getuser()

# COMMAND ----------

spark.read.parquet(f'/user/{username}/retail_db_parquet/orders').show()

# COMMAND ----------

spark.read.parquet(f'/user/{username}/retail_db_parquet/orders').dtypes

# COMMAND ----------

schema = """
    order_id INT,
    order_date TIMESTAMP,
    order_customer_id INT,
    order_status STRING
  """

# COMMAND ----------

# This will run faster as data will not be read to infer the schema
# Fail to convert order_id and order_customer_id as int
spark.read.schema(schema).parquet(f'/user/{username}/retail_db_parquet/orders').show()

# COMMAND ----------

schema = """
    order_id BIGINT,
    order_date TIMESTAMP,
    order_customer_id BIGINT,
    order_status STRING
  """

# COMMAND ----------

spark.read.schema(schema).parquet(f'/user/{username}/retail_db_parquet/orders').show()

# COMMAND ----------

schema = """
    order_id BIGINT,
    order_date STRING,
    order_customer_id BIGINT,
    order_status STRING
  """

# COMMAND ----------

spark.read.schema(schema).parquet(f'/user/{username}/retail_db_parquet/orders').show()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

schema = StructType([
    StructField('order_id', IntegerType()),
    StructField('order_date', StringType()),
    StructField('order_customer_id', IntegerType()),
    StructField('order_status', StringType())
])

# COMMAND ----------

spark.read.schema(schema).parquet(f'/user/{username}/retail_db_parquet/orders').show()

# COMMAND ----------

schema = StructType([
    StructField('order_id', LongType()),
    StructField('order_date', StringType()),
    StructField('order_customer_id', LongType()),
    StructField('order_status', StringType())
])

# COMMAND ----------

orders = spark.read.schema(schema).parquet(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

orders.show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

orders.\
    withColumn('order_date', col('order_date').cast('timestamp')).dtypes

# COMMAND ----------

orders.show(truncate = False)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


