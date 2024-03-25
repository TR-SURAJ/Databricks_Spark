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

emp.withColumn("full_name", concat("first_name","last_name")).show()

# COMMAND ----------

help(concat_ws)

# COMMAND ----------

emp.withColumn("full_name", concat_ws("-","first_name","last_name")).show()

# COMMAND ----------

emp.withColumn("full_name", concat_ws(lit(", "),"first_name","last_name")).show()

# COMMAND ----------

address = [
    {
        "id": 10,
        "address": "9 debs parkway",
        "city": "new york city",
        "state": "new york",
        "country": "US",
        "postal_code": "10090"
    },
    {
        "id": 11,
        "address": "3645 dayton hill",
        "city": "newton",
        "state": "massachusetts",
        "country": "US",
        "postal_code": "02162"
    },
    {
        "id": 12,
        "address": "1551 6th plaza",
        "city": "modesto",
        "state": "california",
        "country": "US",
        "postal_code": "95354"
    },
    {
        "id": 13,
        "address": "7849 ohio drive",
        "city": "springfield",
        "state": "missouri",
        "country": "US",
        "postal_code": "10120"
    }
]

# COMMAND ----------

addressdf = spark.createDataFrame(address)

# COMMAND ----------

addressdf.show()

# COMMAND ----------

addressdf.select('id', concat_ws(', ', 'address','city','state','country','postal_code').alias('full_address')).show(truncate = False)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

help(initcap)

# COMMAND ----------

help(length)

# COMMAND ----------

addressdf.select('id','state'). \
    withColumn("state_upper", upper('state')).\
    withColumn("state_lower", lower('state')). \
    withColumn("state_initcap", initcap("state")).\
    withColumn("state_length", length("state")).show()

# COMMAND ----------


