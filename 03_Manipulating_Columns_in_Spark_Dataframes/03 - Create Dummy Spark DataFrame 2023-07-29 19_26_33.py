# Databricks notebook source
l  = [('X',)]

# COMMAND ----------

df = spark.createDataFrame(l, "dummy string")

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

l = [('X',)]

# COMMAND ----------

df = spark.createDataFrame(l,"dummy STRING")

# COMMAND ----------

df.show()

# COMMAND ----------

df.select(current_date()).show()

# COMMAND ----------

df.select(current_date().alias("current_date")).show()

# COMMAND ----------

employee_list = [(1,"bob","jackson",1500.0,"AUSTRALIA","+61 9890989789","345 23 5645"),
            (2,"hill","martin",750.0,"UK","+44 3465789709","390 45 7598")]

# COMMAND ----------

emp = spark.createDataFrame(employee_list, schema = """employee_id INT, first_name STRING, last_name STRING, salary FLOAT, nationality STRING,
                            phone_number STRING, ssn STRING """)

# COMMAND ----------

emp.show()

# COMMAND ----------

emp = spark.createDataFrame(employee_list, schema = 'employee_id INT, first_name STRING, last_name STRING, salary FLOAT, nationality STRING, \
                            phone_number STRING, ssn STRING ')

# COMMAND ----------

emp.show()

# COMMAND ----------

emp.show(truncate=False)

# COMMAND ----------


