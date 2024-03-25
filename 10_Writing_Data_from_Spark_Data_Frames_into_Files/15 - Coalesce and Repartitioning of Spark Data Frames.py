# Databricks notebook source
# MAGIC %md
# MAGIC - `coalesce` and `repartition` are functions on top of the dataframe. Do not get confused between **coalesce** on DataFrame and the coalsece function available to deal with null in a given col
# MAGIC - `coalesce` is typically used to **reduce number of partitions** to deal with as part of downstream processing
# MAGIC - `repartition` is used to reshuffle the data to **higher or lower number of partitions** to deal with as part of downstream processing
# MAGIC - Make sure to use a cluster with higher configuration, if you would like to run and experience by your self.

# COMMAND ----------

df = spark.read.csv('dbfs:/databricks-datasets/asa/airlines', header=True)

# COMMAND ----------

help(df.coalesce)

# COMMAND ----------

help(df.repartition)

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC - `repartition` incurs shuffling and it takes time as data has to be shuffled to newer number of partitions
# MAGIC - Also you can `repartition` the DataFrame based on specified columns
# MAGIC - `coalesce` does not incur shuffling
# MAGIC - We use `coalesce` quite often before writing the data to fewer number of files

# COMMAND ----------

df = spark.read.csv('dbfs:/databricks-datasets/asa/airlines', header=True,inferSchema=True)

# COMMAND ----------

dbutils.fs.ls('dbfs:/databricks-datasets/asa/airlines')

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

#coalescing the dataframe to 16

df.coalesce(16).rdd.getNumPartitions()

# COMMAND ----------

# not effective as coalesce can be used to reduce the number of partitions. Faster as no shuffling is involved
df.coalesce(186).rdd.getNumPartitions()

# COMMAND ----------

df.repartition(16).rdd.getNumPartitions()

# COMMAND ----------

df.repartition(186, 'Year', 'Month').rdd.getNumPartitions()

# COMMAND ----------


