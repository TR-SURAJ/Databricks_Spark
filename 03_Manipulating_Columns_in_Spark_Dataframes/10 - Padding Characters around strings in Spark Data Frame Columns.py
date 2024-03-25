# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

help(lpad)

# COMMAND ----------

l = [('X',)]

# COMMAND ----------

df = spark.createDataFrame(l, 'dummy string')

# COMMAND ----------

df.show()

# COMMAND ----------

df.select(lpad(lit("hello"), 10, '-').alias('dummy')).show()

# COMMAND ----------

employees = [
    (1, "Scott", "Tiger", 1000.0, "united states", "+1 123 456 7890", "123 45 6789"),
    (2, "Henry", "Ford", 1250.0, "india", "+91 234 567 8901", "456 78 9123"),
    (3, "Nick", "Junior", 750.0, "united kingdom", "+44 111 111 111", "222 33 4444"),
    (4, "Bill", "Gomes", 1500.0, "australia", "+61 987 654 3210", "789 12 6118"),
]

# COMMAND ----------

empdf = spark.createDataFrame(employees, schema = """employee_id INT, first_name STRING,
                              last_name STRING, salary FLOAT, nationality STRING,
                              phone_number STRING, ssn STRING                              
                               """)

# COMMAND ----------

empdf.show()

# COMMAND ----------

empfixedDf = empdf.select(
    concat(
        lpad("employee_id",5,"0"),
        rpad("first_name",10,"-"),
        rpad("last_name",10,"-"),
        lpad("salary",10,"0"),
        rpad("nationality",15,"-"),
        rpad("phone_number",17,"-"),
        "ssn"
    ).alias("employee")
)

# COMMAND ----------

empfixedDf.show(truncate=False)

# COMMAND ----------


