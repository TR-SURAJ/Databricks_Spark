# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

datetimes = [
    ("20140228", "28-Feb-2014 10:00:00.123"),
    ("20160329", "29-Mar-2016 11:23:00.234"),
    ("20180420", "20-Apr-2018 12:34:00.543"),
    ("20190512", "12-May-2019 13:21:00.567")
]

# COMMAND ----------

datedf = spark.createDataFrame(datetimes, schema = 'date STRING, time STRING')

# COMMAND ----------

datedf.show(truncate=False)

# COMMAND ----------

help(to_date)

# COMMAND ----------

l = [('X',)]

# COMMAND ----------

df = spark.createDataFrame(l, schema = "dummy string")

# COMMAND ----------

df.show()

# COMMAND ----------

df.select(to_date(lit('20230302'), 'yyyyMMdd').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('2021061'), 'yyyyDDD').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02/03/2021'), 'dd/MM/yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02-03-2021'), 'dd-MM-yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02-Mar-2021'), 'dd-MMM-yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('March 2, 2021'), 'MMMM d, yyyy').alias('to_date')).show()

# COMMAND ----------

help(to_timestamp)

# COMMAND ----------

df.select(to_timestamp(lit('02-Mar-2021'), 'dd-MMM-yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_timestamp(lit('02-Mar-2021 17:30:15'), 'dd-MMM-yyyy HH:mm:ss').alias('to_date')).show()

# COMMAND ----------

datedf.printSchema()

# COMMAND ----------

datedf.show(truncate=False)

# COMMAND ----------

datedf. \
    withColumn("to_date", to_date('date', 'yyyyMMdd')). \
    withColumn("to_timestamp", to_timestamp(col('time'), 'dd-MMM-yyyy HH:mm:ss.SSS')).\
    show(truncate = False)

# COMMAND ----------


