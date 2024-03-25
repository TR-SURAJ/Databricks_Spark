# Databricks notebook source
import datetime

users = [(1,
          'Corrie',
          'Van den oord',
          'cvandenoord@etsy.com',
          True,
          550.55,
          datetime.date(2021,1,15),
          datetime.datetime(2021,2,10,1,15)),
         (2,
          'Nikolus',
          'Brewitt',
          'nbrewitti@dailymail.co.uk',
          True,
          900.0,
          datetime.date(2021,2,14),
          datetime.datetime(2021,2,18,3,33)),
         (3,
          'Orelie',
          'Penney',
          'openney2@vistaprint.com',
          True,
          850.55,
          datetime.date(2021,1,21),
          datetime.datetime(2021,3,15,15,16,55)),
         (4,
          'Ashby',
          'Maddocks',
          'amaddocks3@home.pl',
          False,
          None,
          None,
          datetime.datetime(2021,4,10,17,45,30)),
         (5,
          'kurt',
          'Roem',
          'krome4@shutterfly.com',
          False,
          None,
          None,
          datetime.datetime(2021,4,2,0,55,18))
             
         ]

# COMMAND ----------

users_schema = [
    'id',
    'first_name',
    'last_name',
    'email',
    'is_customer',
    'amount_paid',
    'customer_from',
    'last_updated_ts'
]

# COMMAND ----------

spark.createDataFrame(users,schema = users_schema)

# COMMAND ----------

spark.createDataFrame(users,schema = users_schema).show()

# COMMAND ----------

