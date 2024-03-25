# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.functions import *

# COMMAND ----------

import pandas as pd
spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', False)

# COMMAND ----------

import datetime

users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",        
        "email": "cvandenoor@etsy.com",
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021,1,15),
        "last_updated_ts": datetime.datetime(2021,2,10,1,15,0)
    },
    {
        "id": 2,
        "first_name": "John",
        "last_name": "Cena",        
        "email": "john@cena.com",        
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2022,5,15),
        "last_updated_ts": datetime.datetime(2024,3,15,1,16,0)
    },
    {
        "id": 3,
        "first_name": "James",
        "last_name": "Bond",        
        "email": "james@bond.com",        
        "is_customer": False,
        "amount_paid": 750.60,
        "customer_from": datetime.date(2023,1,12),
        "last_updated_ts": datetime.datetime(2018,5,5,5,17,2)
    },
    {
        "id": 3,
        "first_name": "James",
        "last_name": "Bond",        
        "email": "james@bond.com",        
        "is_customer": False,
        "amount_paid": 750.60,
        "customer_from": datetime.date(2023,1,12),
        "last_updated_ts": datetime.datetime(2018,5,5,5,17,2)
    },
    {
        "id": 4,
        "first_name": "Robert",
        "last_name": "Dowrey",        
        "email": "robert@dowrey.com",        
        "is_customer": True,
        "amount_paid": 500.50,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2019,4,3,8,14,8)
    },
    {
        "id": 4,
        "first_name": "Robert",
        "last_name": "Dowrey",        
        "email": "robert@dowrey.com",        
        "is_customer": True,
        "amount_paid": 500.50,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2019,4,3,8,14,8)
    },
    {
        "id": 5,
        "first_name": "Chris",
        "last_name": "Hemmsworth",        
        "email": "chris@hemmsworth.com",        
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2019,4,3,8,14,8)
    },
    {
        "id": 5,
        "first_name": "Chris",
        "last_name": "Hemmsworth",        
        "email": "chris@hemmsworth.com",        
        "is_customer": False,
        "amount_paid": 500.0,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2018,3,4,6,12,6)
    },
]

# COMMAND ----------

userdf = spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

userdf.show(truncate=False)

# COMMAND ----------

userdf.count()

# COMMAND ----------

help(userdf.distinct)

# COMMAND ----------

userdf.distinct().count()

# COMMAND ----------

help(userdf.drop_duplicates)

# COMMAND ----------

help(userdf.dropDuplicates)

# COMMAND ----------

# MAGIC %md
# MAGIC - We can also drop duplicates based on certain columns
# MAGIC - The below operation will fail as the function expects sequence type object such as list or array

# COMMAND ----------

userdf.dropDuplicates('id').show()

# COMMAND ----------

userdf.show(truncate=False)

# COMMAND ----------

userdf.dropDuplicates(['id']).show()

# COMMAND ----------

userdf.dropDuplicates(['id','amount_paid']).show()

# COMMAND ----------


