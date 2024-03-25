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
        "id": "1",
        "first_name": "Corrie",
        "last_name": "Van den Oord",        
        "email": "cvandenoor@etsy.com",
        "is_customer": True,
        "amount_paid": "1000.55",
        "customer_from": datetime.date(2021,1,15),
        "last_updated_ts": datetime.datetime(2021,2,10,1,15,0)
    },
    {
        "id": None,
        "first_name": "John",
        "last_name": "Cena",        
        "email": "john@cena.com",        
        "is_customer": True,
        "amount_paid": "900.0",
        "customer_from": datetime.date(2022,5,15),
        "last_updated_ts": datetime.datetime(2024,3,15,1,16,0)
    },
    {
        "id": "3",
        "first_name": "James",
        "last_name": "Bond",        
        "email": "james@bond.com",        
        "is_customer": False,
        "amount_paid": "750.60",
        "customer_from": datetime.date(2023,1,12),
        "last_updated_ts": datetime.datetime(2018,5,5,5,17,2)
    },
    {
        "id": "3",
        "first_name": "James",
        "last_name": "Bond",        
        "email": "james@bond.com",        
        "is_customer": False,
        "amount_paid": "750.60",
        "customer_from": datetime.date(2023,1,12),
        "last_updated_ts": datetime.datetime(2018,5,5,5,17,2)
    },
    {
        "id": "4",
        "first_name": "Robert",
        "last_name": "Dowrey",        
        "email": "robert@dowrey.com",        
        "is_customer": True,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2019,4,3,8,14,8)
    },    
    {
        "id": "5",
        "first_name": "Chris",
        "last_name": "Hemmsworth",        
        "email": "chris@hemmsworth.com",        
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2019,4,3,8,14,8)
    },
    {
        "id": None,
        "first_name": None,
        "last_name": None,        
        "email": None,        
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": None,
        "first_name": None,
        "last_name": None,        
        "email": None,        
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": "6",
        "first_name": "ashby",
        "last_name": "maddock",        
        "email": "ashby@maddock.com",        
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2023,1,1,3,12,7)
    },
    {
        "id": "7",
        "first_name": None,
        "last_name": "maddock",        
        "email": None,        
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2022,4,9,1,11,9)
    },
    {
        "id": "8",
        "first_name": None,
        "last_name": None,        
        "email": None,        
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
]

# COMMAND ----------

userdf = spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

userdf.show(truncate=False)


# COMMAND ----------

userdf.count()

# COMMAND ----------

help(userdf.dropna)

# COMMAND ----------

#drops rows which have all columns as null
userdf.dropna('all').show()

# COMMAND ----------

# drop if any column value is null
userdf.dropna('any').show()

# COMMAND ----------

userdf.dropna(thresh=4).show() # <= thresh

# COMMAND ----------

userdf.dropna(how = 'all', subset = ['id', 'email']).show() # if only the subset columns have null values the rows will be dropped (AND)

# COMMAND ----------

userdf.dropna(how = 'any', subset = ['id', 'email']).show() # drops record if any of the two subset columns have null (OR)

# COMMAND ----------


