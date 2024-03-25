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
        "courses": "Intermediate Java",
        "suitable_for": "Intermediate",        
        "number_of_ratings": 35000,
        "customer_from": datetime.date(2021,1,15),
        "last_updated_ts": datetime.datetime(2021,2,10,1,15,0)
    },
    {
        "id": 2,
        "first_name": "John",
        "last_name": "Cena",        
        "email": "john@cena.com",        
        "courses": "Intermediate Python",
        "suitable_for": "Intermediate",        
        "number_of_ratings": 43000,
        "customer_from": datetime.date(2022,5,15),
        "last_updated_ts": datetime.datetime(2024,3,15,1,16,0)
    },
    {
        "id": 3,
        "first_name": "James",
        "last_name": "Bond",        
        "email": "james@bond.com",        
        "courses": "Intermediate C++",
        "suitable_for": "Intermediate",       
        "number_of_ratings": 12000,
        "customer_from": datetime.date(2023,1,12),
        "last_updated_ts": datetime.datetime(2018,5,5,5,17,2)
    },
    {
        "id": 4,
        "first_name": "Robert",
        "last_name": "Dowrey",        
        "email": "robert@dowrey.com",        
        "courses": "Beginner Java",
        "suitable_for": "Beginner",
        "number_of_ratings": 4000,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2019,4,3,8,14,8)
    },
    {
        "id": 5,
        "first_name": "Chris",
        "last_name": "Hemmsworth",        
        "email": "chris@hemmsworth.com",        
        "courses": "Beginner Python",
        "suitable_for": "Beginner",
        "number_of_ratings": 35000,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2019,4,3,8,14,8)
    },
    {
        "id": 6,
        "first_name": "Dan",
        "last_name": "Brownie",        
        "email": "dan@brownie.com",        
        "courses": "Advanced System Design",
        "suitable_for": "Advanced",
        "number_of_ratings": 9000,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2019,4,3,8,14,8)
    },
]

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

userdf = spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

userdf.show(truncate=False)

# COMMAND ----------


