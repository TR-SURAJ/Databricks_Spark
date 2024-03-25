# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

datetimes = [
    ("2014-02-28", "2014-02-28 10:00:00.123"),
    ("2016-03-29", "2016-03-29 11:23:00.234"),
    ("2018-04-20", "2018-04-20 12:34:00.543"),
    ("2019-05-12", "2019-05-12 13:21:00.567")
]

# COMMAND ----------

datedf = spark.createDataFrame(datetimes, schema = 'date STRING, time STRING')

# COMMAND ----------

datedf.show(truncate=False)

# COMMAND ----------

help(date_format)

# COMMAND ----------

# MAGIC %md 
# MAGIC - Get the year and month from both date and time columns using yyyyMM format. Also make sure that the data type is converted to integer

# COMMAND ----------

datedf. \
    withColumn("date_ym", date_format("date", "yyyyMM")). \
    withColumn("time_ym", date_format("time", "yyyyMM")). \
    show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - yyyy : year
# MAGIC - MM   : month
# MAGIC - dd   : day of the month
# MAGIC - DD   : Julian day (day of the year)
# MAGIC - HH   : 24 hour format
# MAGIC - hh   : 12 hour format
# MAGIC - mm   : minute
# MAGIC - ss   : second
# MAGIC - SSS  : milli second

# COMMAND ----------

datedf. \
    withColumn("date_ym", date_format("date", "yyyyMM").cast('int')). \
    withColumn("time_ym", date_format("time", "yyyyMM").cast('int')). \
    show(truncate=False)

# COMMAND ----------

datedf. \
    withColumn("date_dt", date_format("date", "yyyyMMddHHmmss")). \
    withColumn("time_ts", date_format("time", "yyyyMMddHHmmss")). \
    show(truncate=False)

# COMMAND ----------

datedf. \
    withColumn("date_yd", date_format("date", "yyyyDDD")). \
    withColumn("time_yd", date_format("time", "yyyyDDD")). \
    show(truncate=False)

# COMMAND ----------

datedf. \
    withColumn("date_yd", date_format("date", "MMMM d, yyyy")). \
    show(truncate=False)

# COMMAND ----------

# name of weekday
datedf. \
    withColumn("date_abbr", date_format("date", "EE")). \
    show(truncate=False)

# COMMAND ----------

# full name of weekday
datedf. \
    withColumn("date_abbr", date_format("date", "EEEE")). \
    show(truncate=False)

# COMMAND ----------


