# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

employees = [
    (1, "scott", "Tiger", 1000.0,30,"united states", "+1 123 456 7890", "123 45 6789"),
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

empdf. \
    withColumn('bonus1', coalesce(col('bonus').cast('int'), lit(0))). \
    show()

# COMMAND ----------

empdf. \
    withColumn(
        'bonus1',
        expr("""
             CASE
                WHEN bonus is NULL OR bonus = '' THEN 0
                ELSE bonus
             END             
              """)
    ).show()

# COMMAND ----------

empdf. \
    withColumn(
        'bonus1',
        when( (col('bonus').isNull()) | (col('bonus') == lit('')), 0).otherwise(col('bonus'))
    ). \
    show()

# COMMAND ----------

persons = [
    (1,1),
    (2,3),
    (3,18),
    (4,60),
    (5,120),
    (6,0),
    (7,12),
    (8,160)
]

# COMMAND ----------

personsdf = spark.createDataFrame(persons, schema = 'id INT, age INT')

# COMMAND ----------

personsdf.show()

# COMMAND ----------

personsdf. \
    withColumn('category',
               expr("""
                    CASE
                        WHEN age BETWEEN 0 AND 2 THEN 'New Born'
                        WHEN age > 2 AND age <= 12 THEN 'Infant'
                        WHEN age > 12 AND age <= 48 THEN 'Toddler'
                        WHEN age > 48 AND age <=144 THEN 'Kid'
                        ELSE 'Tennager/Adult'
                    END
             """)
    ).show()

# COMMAND ----------

personsdf. \
    withColumn(
        "category",
         when(col('age').between(0,2), 'New Born').
         when( (col('age') > 2) & (col('age') <= 12), "Infant").
         when( (col('age') > 12) & (col('age') <= 48), "Toddler").
         when( (col('age') > 48) & (col('age') <= 144), "Kid").
         otherwise('Tennager/Adult')
    ).\
    show()


# COMMAND ----------


