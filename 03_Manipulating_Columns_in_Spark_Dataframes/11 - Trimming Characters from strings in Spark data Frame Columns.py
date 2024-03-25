# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

l = [("     Hello.      ",)]

# COMMAND ----------

df = spark.createDataFrame(l).toDF("dummy")

# COMMAND ----------

df.show()

# COMMAND ----------

help(trim)

# COMMAND ----------

help(ltrim)

# COMMAND ----------

df.withColumn("ltrim", ltrim("dummy")). \
   withColumn("rtrim", rtrim("dummy")). \
   withColumn("trim", trim("dummy")). \
   show()

# COMMAND ----------

spark.sql('DESCRIBE FUNCTION rtrim').show(truncate=False)

# COMMAND ----------

df.withColumn("ltrim", expr("ltrim(dummy)")). \
   withColumn("rtrim", expr("rtrim('.', rtrim(dummy))")). \
   withColumn("trim", trim(col("dummy"))). \
   show()

# COMMAND ----------

spark.sql('DESCRIBE FUNCTION trim').show(truncate=False)

# COMMAND ----------

df.withColumn("ltrim", expr("trim(LEADING " " FROM dummy)")). \
   withColumn("rtrim", expr("trim(TRAILING '.' FROM rtrim(dummy))")). \
   withColumn("trim", expr("trim(BOTH ' ' FROM dummy)")). \
   show()

# COMMAND ----------


