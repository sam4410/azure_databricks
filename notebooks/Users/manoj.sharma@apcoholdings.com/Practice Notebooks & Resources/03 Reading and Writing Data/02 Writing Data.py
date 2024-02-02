# Databricks notebook source
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField
countries_schema = StructType([
    StructField("COUNTRY_ID", IntegerType(), False),
    StructField("NAME", StringType(), False),
    StructField("NATIONALITY", StringType(), False),
    StructField("COUNTRY_CODE", StringType(), False),
    StructField("ISO_ALPHA2", StringType(), False),
    StructField("CAPITAL", StringType(), False),
    StructField("POPULATION", DoubleType(), False),
    StructField("AREA_KM2", IntegerType(), True),
    StructField("REGION_ID", IntegerType(), True),
    StructField("SUB_REGION_ID", IntegerType(), True),
    StructField("INTERMEDIATE_REGION_ID", IntegerType(), True),
    StructField("ORGANIZATION_REGION_ID", IntegerType(), True)
])

path = '/FileStore/tables/raw/countries.csv'
countries_df = spark.read.options(header = True).schema(countries_schema).csv(path)
display(countries_df)

# COMMAND ----------

# Write the daraframe as csv in DBFS
path = '/FileStore/tables/raw/'

countries_df.write.csv(path + 'countries_out', header = True, mode = 'overwrite')
#countries_df.options(header = True).mode('overwrite').write(path + 'countries_out')

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/raw/countries_out', header = True)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Partitioning the Data

# COMMAND ----------

# Create data partitions by 'region_id'
df.write.options(header = True).mode('overwrite').partitionBy('REGION_ID').csv('/FileStore/tables/raw/countries_out1')

# COMMAND ----------

# Reading the partitioned data
df2 = spark.read.options(header  =True).csv('/FileStore/tables/raw/countries_out1')
display(df2)

# COMMAND ----------

# Reading just a single partition of the entire file
df_part = spark.read.options(header = True).csv('/FileStore/tables/raw/countries_out1/REGION_ID=10')
display(df_part)

# COMMAND ----------

# MAGIC %md
# MAGIC Apache parquet is an open source, column oriented, data file format designed for efficient data storage and retrieval. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk. Parquet is designed to be a common interchange format for both batch and interactive workloads. It is similar to other columnar-storage file formats in Hadoop, namely RCFile and ORC.

# COMMAND ----------

# remove a folder in DBFS
# dbutils.fs.rm('dbfs:/FileStore/tables/rawcountries_out', True)