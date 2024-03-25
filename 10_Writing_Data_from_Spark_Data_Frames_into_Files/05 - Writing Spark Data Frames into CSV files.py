# Databricks notebook source
# MAGIC %md
# MAGIC - We can write data from Sprak Data Frame into CSV files using multiple approches
# MAGIC - Approach 1: df.write.csv('path_to_folder')
# MAGIC - Approach 2: df.write.format('csv').save('path_to_folder')
# MAGIC - The column names from the schema can be added as header to each of the files by saying header=True
# MAGIC - We can also write data into files using characters other than comma as delimiters/seperators
# MAGIC - We can also compress the data while writing into files using csv

# COMMAND ----------

courses = [
    {   
        'course_id': 1,
        'course_name': "Python Bootcamp",
        'suitable_for': 'Beginner',
        'enrollment': 1100093,
        'stars': 4.6,
        'number_of_ratings': 218066
    },
    {   
        'course_id': 2,
        'course_name': "angular - the complete guide",
        'suitable_for': 'Intermediate',
        'enrollment': 34567,
        'stars': 4.5,
        'number_of_ratings': 347912
    },
    {
        'course_id': 3,
        'course_name': "Java In-Depth",
        'suitable_for': 'Adavanced',
        'enrollment': 2345321,
        'stars': 4.8,
        'number_of_ratings': 23789
    },
    {
        'course_id': 4,
        'course_name': "C++ Beginner guide",
        'suitable_for': 'Beginner',
        'enrollment': 32145,
        'stars': 4.2,
        'number_of_ratings': 5678
    },
    {
        'course_id': 5,
        'course_name': "Data Science Practical approach",
        'suitable_for': 'Intermediate',
        'enrollment': 67897,
        'stars': 4.6,
        'number_of_ratings': 267576
    }
]

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Row

# COMMAND ----------

course_df = spark.createDataFrame([Row(**course) for course in courses])

# COMMAND ----------

course_df.show()

# COMMAND ----------

import getpass
username = getpass.getuser()

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/course')

# COMMAND ----------

dbutils.fs.rm(f'/user/{username}/course', recurse=True)

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/course')

# COMMAND ----------

#Default behavior. It will delimit the data using comma as seperator
course_df.write.csv(f'/user/{username}/course')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/course')

# COMMAND ----------

course_df.write.format('csv').save(f'/user/{username}/course',mode = 'overwrite')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/course')

# COMMAND ----------

# Using spark.read.text we can read the raw data as single column dataframe
# We can confirm default delimiter as comma by looking at the data
spark.read.text(f'/user/{username}/course').show(truncate=False)

# COMMAND ----------


