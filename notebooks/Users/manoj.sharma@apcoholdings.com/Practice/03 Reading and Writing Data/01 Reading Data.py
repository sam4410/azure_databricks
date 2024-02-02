# Databricks notebook source
path = '/FileStore/tables/raw/'

countries_df = spark.read.csv(path + 'countries.csv', header=True)
countries_df.show(5)

# COMMAND ----------

print(type(countries_df))

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df = spark.read.options(header = True).csv(path + 'countries.csv')
display(countries_df)

# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

countries_df.schema

# COMMAND ----------

countries_df = spark.read.options(header = True, inferSchema = True).csv(path + 'countries.csv')    #not efficient

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

# create schema for dataframe
countries_schema = StructType([
    StructField("COUNTRY_ID", IntegerType(), False),
    StructField("NAME", StringType(), False),
    StructField("NATIONALITY", StringType(), False),
    StructField("COUNTRY_CODE", StringType(), False),
    StructField("ISO_ALPHA2", StringType(), False),
    StructField("CAPITAL", StringType(), False),
    StructField("POPULATION", DoubleType(), False),
    StructField("AREA_KM2", IntegerType(), False),
    StructField("REGION_ID", IntegerType(), True),
    StructField("SUB_REGION_ID", IntegerType(), True),
    StructField("INTERMEDIATE_REGION_ID", IntegerType(), True),
    StructField("ORGANIZATION_REGION_ID", IntegerType(), True),
])

countries_df = spark.read.options(header = True).schema(countries_schema).csv(path + 'countries.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading JSON data

# COMMAND ----------

path = '/FileStore/tables/raw/'
file_name = 'countries_single_line.json'

countries_sl_json = spark.read.json(path = path + file_name)
display(countries_sl_json)

# COMMAND ----------

path = '/FileStore/tables/raw/'
file_name = 'countries_multi_line.json'

countries_ml_json = spark.read.json(path = path + file_name, multiLine=True)
display(countries_ml_json)

# COMMAND ----------

countries_ml_json = spark.read.options(multiLine=True).json(path = path + file_name)
display(countries_ml_json)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading text data

# COMMAND ----------

path = '/FileStore/tables/raw/'

countries_txt = spark.read.csv(path + 'countries.txt', header = True, sep= '\t')
display(countries_txt)

# COMMAND ----------

countries_txt = spark.read.options(header = True, sep= '\t').csv(path + 'countries.txt')
display(countries_txt)