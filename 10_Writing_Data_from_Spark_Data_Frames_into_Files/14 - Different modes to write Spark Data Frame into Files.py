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

import getpass
username = getpass.getuser()

# COMMAND ----------

help(course_df.write.mode)

# COMMAND ----------

# MAGIC %md
# MAGIC Different ways mode can be specified while writing data frame into files. *file_format* can be any valid out of box format such as text, csv, json, parquet. orc
# MAGIC
# MAGIC - course_df.write.mode(saveMode).file_format(path_to_folder)
# MAGIC - course_df.write.file_format(path_to_folder,mode=saveMode)
# MAGIC - course_df.write.mode(saveMode).format('file_format').save(path_to_folder)
# MAGIC - course_df.write.format('file_format').save(path_to_folder, mode=saveMode)

# COMMAND ----------

# MAGIC %md
# MAGIC - Understand default behavior
# MAGIC   - Fails if folder exists
# MAGIC   - Creates folder and then adds files to it

# COMMAND ----------

dbutils.fs.rm(f'/user/{username}/course', recurse=True)

# COMMAND ----------

course_df.\
    coalesce(1).\
    write.\
    parquet(
        f'/user/{username}/course'
    )

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/course')

# COMMAND ----------

# fails as mode is error or errorIfExists by default
course_df.\
    coalesce(1).\
    write.\
    parquet(
        f'/user/{username}/course'
    )

# COMMAND ----------

spark.read.parquet(f'/user/{username}/course').count()

# COMMAND ----------

course_df.\
    coalesce(1).\
    write.\
    mode('overwrite').\
    parquet(f'/user/{username}/course')

# COMMAND ----------

course_df.\
    coalesce(1).\
    write.\
    parquet(f'/user/{username}/course', mode = 'overwrite')

# COMMAND ----------

course_df.\
    coalesce(1).\
    write.\
    format('parquet').\
    save(f'/user/{username}/course',mode = 'overwrite')

# COMMAND ----------

course_df.\
    coalesce(1).\
    write.\
    mode('append').\
    parquet(f'/user/{username}/course')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/course')

# COMMAND ----------

spark.read.parquet(f'/user/{username}/course').count()

# COMMAND ----------


