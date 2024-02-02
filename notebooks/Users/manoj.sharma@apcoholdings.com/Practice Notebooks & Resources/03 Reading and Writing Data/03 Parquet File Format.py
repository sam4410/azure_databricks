# Databricks notebook source
path = '/FileStore/tables/raw/'
countries_df = spark.read.options(header = True, inferSchema = True).csv(path + 'countries.csv')
display(countries_df)

# COMMAND ----------

#write data into parquet file format
folder_name = 'countries_parquet'
countries_df.write.parquet('/FileStore/tables/raw/' + folder_name)

# COMMAND ----------

# read back the data from parquet file format
display(spark.read.parquet('/FileStore/tables/raw/countries_parquet'))

# COMMAND ----------

