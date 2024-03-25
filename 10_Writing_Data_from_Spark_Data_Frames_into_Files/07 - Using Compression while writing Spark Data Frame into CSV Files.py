# Databricks notebook source
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

dbutils.fs.rm(f'/user/{username}/course', recurse=True)

# COMMAND ----------

course_df.\
    coalesce(1).\
    write.\
    csv(f'/user/{username}/course', mode='overwrite', header=True)

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/course')

# COMMAND ----------

help(course_df.write.csv)

# COMMAND ----------

course_df.\
    coalesce(1).\
    write.\
    csv(f'/user/{username}/course', mode='overwrite', compression='gzip' ,header=True)

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/course')

# COMMAND ----------

spark.read.csv(f'/user/{username}/course', header=True).show()

# COMMAND ----------

course_df.\
    coalesce(1).\
    write.\
    format('csv').\
    save(f'/user/{username}/course', mode='overwrite', compression='snappy' ,header=True)

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/course')

# COMMAND ----------

spark.read.csv(f'/user/{username}/course', header=True).show()

# COMMAND ----------


