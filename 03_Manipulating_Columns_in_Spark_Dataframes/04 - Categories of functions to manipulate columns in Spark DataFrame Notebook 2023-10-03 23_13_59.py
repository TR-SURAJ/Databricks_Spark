# Databricks notebook source
employee_list = [(1,"bob","jackson",1500.0,"AUSTRALIA","+61 9890989789","345 23 5645"),
            (2,"hill","martin",750.0,"UK","+44 3465789709","390 45 7598")]

# COMMAND ----------

emp = spark.createDataFrame(employee_list, schema = """employee_id INT, first_name STRING, last_name STRING, salary FLOAT, nationality STRING,
                            phone_number STRING, ssn STRING """)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

emp.show()

# COMMAND ----------

emp.withColumn("nationality", lower('nationality')).show()

# COMMAND ----------


