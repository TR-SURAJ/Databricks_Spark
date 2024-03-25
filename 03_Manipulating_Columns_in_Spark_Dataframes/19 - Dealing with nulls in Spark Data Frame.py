# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

employees = [
    (1, None, "Tiger", None,30,"united states", "+1 123 456 7890", "123 45 6789"),
    (2, "Henry", "Ford", 1250.0,None, "india", "+91 234 567 8901", "456 78 9123"),
    (3, "Nick", "Junior",500.0,"", "united kingdom", "+44 111 111 111", "222 33 4444"),
    (4, "Bill", "Gomes", 1500.0,150, "australia", "+61 987 654 3210", "789 12 6113"),
]

# COMMAND ----------

empdf = spark.createDataFrame(employees, schema = """employee_id INT, first_name STRING,
                              last_name STRING, salary FLOAT, bonus STRING, nationality STRING,
                              phone_number STRING, ssn STRING                              
                               """)

# COMMAND ----------

empdf.show()

# COMMAND ----------

help(coalesce)

# COMMAND ----------

# Fails because 0 is not passed as column object. But using lit it will look for a column with name 0
empdf. \
    withColumn('bonus1', coalesce('bonus', 0)). \
    show()

# COMMAND ----------

empdf. \
    withColumn('bonus1', coalesce('bonus', lit(0))). \
    show()

# COMMAND ----------

empdf. \
    withColumn('bonus1', col('bonus').cast('int')). \
    show()

# COMMAND ----------

empdf. \
    withColumn('bonus1', coalesce(col('bonus').cast('int'), lit(0))). \
    show()

# COMMAND ----------

empdf. \
    withColumn('bonus1', expr("nvl(bonus,0)")). \
    show()

# COMMAND ----------

empdf. \
    withColumn('bonus1', expr("nvl(nullif(bonus,''),0)")). \
    show()

# COMMAND ----------

empdf. \
    withColumn('payment', col('salary') + ( col('salary') * coalesce(col('bonus'), lit(0)) / 100 )). \
    show()

# COMMAND ----------

empdf. \
    withColumn('payment', col('salary') + ( col('salary') * coalesce(col('bonus').cast('int'), lit(0)) / 100 )). \
    show()

# COMMAND ----------

empdf. \
    withColumn('payment', coalesce(col('salary'),lit(0)) + ( coalesce(col('salary'),lit(0)) * coalesce(col('bonus').cast('int'), lit(0)) / 100 )). \
    show()

# COMMAND ----------

employees = [
    (1, "Scott", None, 1000.0,30,"united states", "+1 123 456 7890", "123 45 6789"),
    (2, "Henry", "Ford", 1250.0, None, "india", "+91 234 567 8901", "456 78 9123"),
    (3, "Nick", None, None,"", "united kingdom", "+44 111 111 111", "222 33 4444"),
    (4, "Bill", "Gomes", 1500.0,150, "australia", "+61 987 654 3210", "789 12 6113"),
]

# COMMAND ----------

empdf = spark.createDataFrame(employees, schema = """employee_id INT, first_name STRING,
                              last_name STRING, salary FLOAT, bonus STRING, nationality STRING,
                              phone_number STRING, ssn STRING                              
                               """)

# COMMAND ----------

empdf.show()

# COMMAND ----------

help(empdf.na)

# COMMAND ----------

help(empdf.na.fill)

# COMMAND ----------

help(empdf.fillna)

# COMMAND ----------

empdf.show()

# COMMAND ----------

empdf.fillna(0.0).show() # since 0.0 is float it fills with that value only in float columns

# COMMAND ----------

empdf.fillna("na").show()

# COMMAND ----------

empdf.fillna(0.0).fillna("na").show()

# COMMAND ----------

empdf.fillna(0.0, 'salary').fillna("na", 'last_name').show()

# COMMAND ----------


