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

emp.groupBy("nationality").count().show()

# COMMAND ----------

emp.orderBy("employee_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - If there are no transformations on any column in any function then we shold be able to pass all column names as string
# MAGIC
# MAGIC - If we have to invoke functions such as cast,desc, etc to customize the behaviour then we have to build column object using col

# COMMAND ----------

help(col)

# COMMAND ----------

type(col('first_name'))

# COMMAND ----------

emp.select(col('first_name'),col('last_name')).show()

# COMMAND ----------

help(upper)

# COMMAND ----------

type(upper('first_name'))

# COMMAND ----------

emp.select(upper("first_name"),upper("last_name")).show()

# COMMAND ----------

emp.select(upper(col("first_name")),upper(col("last_name"))).show()

# COMMAND ----------

emp.groupBy(upper("nationality").alias('nationality')).count().show()

# COMMAND ----------

from pyspark.sql.column import Column
help(Column)

# COMMAND ----------

emp.orderBy("employee_id".desc()).show()

# COMMAND ----------

emp.orderBy(col("employee_id").desc()).show()

# COMMAND ----------

# we are not projecting the col, so its not changes to upeer case
emp.orderBy(upper(emp['first_name']).alias('first_name')).show()

# COMMAND ----------

# we are not projecting the col, so its not changes to upeer case
emp.orderBy(upper(emp.first_name).alias('first_name')).show()

# COMMAND ----------

emp. \
    select(concat(col('first_name'),", ",col('last_name'))).show()

# COMMAND ----------

emp. \
    select(concat(emp['first_name'],", ",emp['last_name'])).show()

# COMMAND ----------

help(lit)

# COMMAND ----------

help(concat)

# COMMAND ----------

# doesnt work, we have to use both col and lit 

emp.withColumn('bonus', 'salary' * lit(0.2)).show()

# COMMAND ----------

emp.withColumn('bonus', col('salary') * lit(0.2)).show()

# COMMAND ----------


