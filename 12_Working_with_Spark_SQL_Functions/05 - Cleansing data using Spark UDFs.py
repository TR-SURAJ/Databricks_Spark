# Databricks notebook source
import pandas as pd

# COMMAND ----------

courses = {
    'course_id': ['1','2','3','4','5','6','7','8','9','10'],
    'course_name': ['Mastering SQL', 'Streaming Pipelines - Python', 'Head First Python', 'Designing Data-Intensive Applications', 'Distributed Systems', 'Database Internals', 'Art of Immutable Architecture', 'Graph Databases', 'Building Microservices', 'Kubernetes Patterns'],
    'course_author': ['Mike Jack', 'Bob Davis', 'Elvis Preseley', 'Martin Kleppmann', 'Sukumar Ghosh', 'Alex Petrov', 'Michale L. Perry', 'Ian Robinson', 'Sam Newman', 'Rolan Hub'],
    'course_status': [' published   ', '    inactive  ', '\\N', 'published ','\\N','   inactive', 'published    ','\\N', ' inactive ', 'published  '],
    'course_published_dt': ['2020-070-08','2023-03-10','\\N','2021-02-27','\\N','2021-05-14','2021-04-18','\\N','2020-12-15','2021-07-11']
}

# COMMAND ----------

courses_df = spark.createDataFrame(pd.DataFrame(courses))

# COMMAND ----------

courses_df.show()

# COMMAND ----------

def data_cleanse(c):
    return c.strip() if c.strip() != '\\N' else None

# COMMAND ----------

data_cleanse = spark.udf.register('data_cleanse', data_cleanse)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

courses_df.select(
    data_cleanse(col('course_status')).alias('course_status'),
    data_cleanse(col('course_published_dt')).alias('course_published_dt')
).show()

# COMMAND ----------

courses_df.createOrReplaceTempView('courseess')

# COMMAND ----------

spark.sql('''
    SELECT course_id, data_cleanse(course_status) as course_status,data_cleanse(course_published_dt) as course_published_dt
    FROM courseess     
   ''').show()

# COMMAND ----------


