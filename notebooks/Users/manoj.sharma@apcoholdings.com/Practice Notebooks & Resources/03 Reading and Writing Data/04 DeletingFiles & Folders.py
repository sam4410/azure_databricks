# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

base_path = '/FileStore/tables/raw/'
dbutils.fs.rm(base_path + 'countries.txt')

# COMMAND ----------

dbutils.fs.rm(base_path + 'countries_multi_line.json')
dbutils.fs.rm(base_path + 'countries_single_line.json')

# COMMAND ----------

dbutils.fs.rm(base_path + 'countries_out', recurse = True)   #deleting a directory/folder

# COMMAND ----------

dbutils.fs.rm(base_path + 'countries_out1', recurse = True)

# COMMAND ----------

dbutils.fs.rm(base_path + 'countries_parquet', recurse = True)

# COMMAND ----------

