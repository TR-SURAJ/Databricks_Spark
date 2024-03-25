# Databricks notebook source
from pyspark.sql import Row

# COMMAND ----------

import datetime

users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoor@etsy.com",
        "phone_numbers": Row(mobile = "+91 8645879087",home = "+91 9878673289"),
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
        "phone_numbers": Row(mobile = "+91 9886879087",home = "+91 9134673289"),
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
        "phone_numbers": Row(mobile = "+91 3245879087",home = "+91 9854673289"),
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
        "phone_numbers": None,
        "is_customer": True,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2019,4,3,8,14,8)
    },
    {
        "id": 5,
        "first_name": "Chris",
        "last_name": "Hemmsworth",
        "email": "chris@hemmsworth.com",
        "phone_numbers": Row(mobile = "+91 9085879087", home = None),
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2019,4,3,8,14,8)
    },
]

# COMMAND ----------

users_df = spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.select('id','phone_numbers').show(truncate=False)

# COMMAND ----------

users_df.dtypes

# COMMAND ----------

users_df.select('id','phone_numbers.mobile','phone_numbers.home').show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df.select('id',col('phone_numbers')['mobile'], col('phone_numbers')['home']).show()

# COMMAND ----------

users_df.select('id','phone_numbers.*').show()

# COMMAND ----------


