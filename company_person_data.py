# Databricks notebook source
!pip install dbldatagen

# COMMAND ----------

!pip install faker

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import StringType
from faker import Faker

# Initialize Faker
fake = Faker()

# Generate random data using Faker
row_count = 100*10
companies = [fake.company() for _ in range(row_count)]
person_names = [fake.name() for _ in range(row_count)]


# Define the data specification using dbldatagen
dataSpec = (
    dg.DataGenerator(spark, name="company_person_data", rows=row_count, partitions=4)
    .withIdOutput()
    .withColumn("company", StringType(), values=companies)
    .withColumn("person_name", StringType(), values=person_names)
)

# Build the DataFrame
df = dataSpec.build()

# Display the DataFrame
df.display()

# COMMAND ----------


