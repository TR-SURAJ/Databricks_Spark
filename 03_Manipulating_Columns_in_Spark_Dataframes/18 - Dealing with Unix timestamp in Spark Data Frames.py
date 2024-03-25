# Databricks notebook source
# MAGIC %md
# MAGIC - We can use unix_timestamp to convert regular date or timestamp to a unix timestamp value

# COMMAND ----------

# MAGIC %md
# MAGIC - We can use from_unixtime to convert unix timestamp to regular date or timestamp

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

datetimes = [
    (20140228,"2014-02-28", "2014-02-28 10:00:00.123"),
    (20160329,"2016-03-29", "2016-03-29 11:23:00.234"),
    (20180420,"2018-04-20", "2018-04-20 12:34:00.543"),
    (20190512,"2019-05-12", "2019-05-12 13:21:00.567")
]

# COMMAND ----------

datedf = spark.createDataFrame(datetimes).toDF("dateid","date","time")

# COMMAND ----------

datedf.show(truncate=False)

# COMMAND ----------

help(unix_timestamp)

# COMMAND ----------

datedf. \
    withColumn("unix_date_id", unix_timestamp(col("dateid").cast("string"), "yyyyMMdd")). \
    withColumn("unix_date", unix_timestamp("date", "yyyy-MM-dd")). \
    show(truncate=False)

# COMMAND ----------

unixtimes = [
    (1393545600, ),
    (1459209600, ),
    (1524182400, ),
    (1557619200, ),
]

# COMMAND ----------

unixtimedf = spark.createDataFrame(unixtimes).toDF("unixtime")

# COMMAND ----------

unixtimedf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC - Get date in yyyyMMdd format and also complete timestamp

# COMMAND ----------

unixtimedf. \
    withColumn("date", from_unixtime("unixtime", "yyyyMMdd")). \
    withColumn("time", from_unixtime("unixtime", "yyyy-MM-dd HH:mm:ss.SSS")). \
    show(truncate=False)     

# COMMAND ----------

# MAGIC %md
# MAGIC - Unix epoch cannot be casted to date, hence this fails

# COMMAND ----------

unixtimedf.select(col('unixtime').cast('date')).show()

# COMMAND ----------

unixtimedf.select(col('unixtime').cast('timestamp')).show()

# COMMAND ----------


