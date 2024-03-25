# Databricks notebook source
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
        "id": 4,
        "first_name": "Robert",
        "last_name": "Dowrey",
        "email": "robert@dowrey.com",
        "is_customer": True,    
        "last_updated_ts": datetime.datetime(2019,4,3,8,14,8)
    },
    {
        "id": 5,
        "first_name": "Chris",
        "last_name": "Hemmsworth",
        "email": "chris@hemmsworth.com",
        "is_customer": False,        
        "last_updated_ts": datetime.datetime(2019,4,3,8,14,8)
    },
]

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

users_df = spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

users_df.show()

# COMMAND ----------

import pandas as pd

# COMMAND ----------

pd.DataFrame(users)

# COMMAND ----------

spark.createDataFrame(pd.DataFrame(users)).show()

# COMMAND ----------


