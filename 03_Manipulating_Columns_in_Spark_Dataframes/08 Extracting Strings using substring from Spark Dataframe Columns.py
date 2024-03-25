# Databricks notebook source
s = "hello suraj"

# COMMAND ----------

s[:3]

# COMMAND ----------

help(substring)

# COMMAND ----------

l = [("X",)]

# COMMAND ----------

df = spark.createDataFrame(l, "dummy string")

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(substring(lit("Hello World"),1,4)).show()

# COMMAND ----------

employees = [
    (1, "Scott", "Tiger", 1000.0, "united states", "+1 123 456 7890", "123 45 6789"),
    (2, "Henry", "Ford", 1250.0, "india", "+91 234 567 8901", "456 78 9123"),
    (3, "Nick", "Junior", 750.0, "united kingdom", "+44 111 111 111", "222 33 4444"),
    (4, "Bill", "Gomes", 1500.0, "australia", "+61 987 654 3210", "789 12 6113"),
]

# COMMAND ----------

empdf = spark.createDataFrame(employees, schema = """employee_id INT, first_name STRING,
                              last_name STRING, salary FLOAT, nationality STRING,
                              phone_number STRING, ssn STRING                              
                               """)

# COMMAND ----------

empdf.show()

# COMMAND ----------

empdf. \
    select("employee_id","phone_number","ssn"). \
    withColumn("phone_last", substring(col('phone_number'),-4,4).cast('int')). \
    withColumn("ssn_last4", substring(col('ssn'),8,4).cast('int')).\
    show()

# COMMAND ----------

empdf. \
    select("employee_id","phone_number","ssn"). \
    withColumn("phone_last", substring('phone_number',-4,4).cast('int')). \
    withColumn("ssn_last4", substring('ssn',8,4).cast('int')).\
    show()

# COMMAND ----------


