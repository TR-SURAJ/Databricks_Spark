# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

help(date_format)

# COMMAND ----------

help(col)

# COMMAND ----------

help(lit)

# COMMAND ----------

help(concat)

# COMMAND ----------

df = spark.createDataFrame([([1, 2], [3, 4], [5]), ([1, 2], None, [3])], ['a', 'b', 'c'])

# COMMAND ----------

df.show()

# COMMAND ----------

df.select(concat(df.a, df.b, df.c).alias("arr")).show()

# COMMAND ----------

help(concat_ws)

# COMMAND ----------


