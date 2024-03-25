# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

help(split)

# COMMAND ----------

help(explode)

# COMMAND ----------

l = [('X',)]

# COMMAND ----------

df = spark.createDataFrame(l,"dummy string")

# COMMAND ----------

df.show()

# COMMAND ----------

df.select(split(lit("Hello world, how are you")," ").alias("word")).show(truncate=False)

# COMMAND ----------

df.select(explode(split(lit("Hello world, how are you")," ").alias('word'))).show()

# COMMAND ----------

employees = [
    (1, "Scott", "Tiger", 1000.0, "united states", "+1 123 456 7890,+1 234 567 8901", "123 45 6789"),
    (2, "Henry", "Ford", 1250.0, "india", "+91 234 567 8901", "456 78 9123"),
    (3, "Nick", "Junior", 750.0, "united kingdom", "+44 111 111 111,+44 222 222 2222", "222 33 4444"),
    (4, "Bill", "Gomes", 1500.0, "australia", "+61 987 654 3210,+61 876 543 2109", "789 12 6118"),
]

# COMMAND ----------

empdf = spark.createDataFrame(employees, schema = """employee_id INT, first_name STRING,
                              last_name STRING, salary FLOAT, nationality STRING,
                              phone_number STRING, ssn STRING                              
                               """)

# COMMAND ----------

empdf.show(truncate=False)

# COMMAND ----------

empdf.select('employee_id','phone_number').show(truncate = False)

# COMMAND ----------

empdf. \
    select('employee_id','phone_number','ssn'). \
    withColumn('phone_numbers', explode(split('phone_number',","))).show(truncate=False)

# COMMAND ----------

empdf.\
    select('employee_id','phone_number','ssn'). \
    withColumn('phone_numbers', explode(split('phone_number',","))). \
    withColumn('area_code', split('phone_numbers'," ")[1].cast('int')). \
    withColumn('phone_last4', split('phone_numbers'," ")[3].cast('int')). \
    withColumn('ssn_last4', split('ssn'," ")[2].cast('int')).\
    show()

# COMMAND ----------


