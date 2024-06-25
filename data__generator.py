# Databricks notebook source
!pip install dbldatagen

# COMMAND ----------

!pip install faker

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import IntegerType, StringType, FloatType, DateType
from faker import Faker

fake = Faker()

def generate_fake_names(no_of_rows):
    first_names = [fake.first_name() for _ in range(no_of_rows)]
    last_names = [fake.last_name() for _ in range(no_of_rows)]
    return first_names, last_names

def generate_customers(no_of_rows):
    first_names, last_names = generate_fake_names(no_of_rows)
    
    customer_spec = (
        dg.DataGenerator(spark, name="customers", rows=no_of_rows, partitions=4)  # Specify number of rows to generate, Specify number of Spark partitions to distribute data generation across
        .withIdOutput()
        .withColumn("CustomerID", IntegerType(), expr="id")  # Generate column data from one or more seed columns
        .withColumn("FirstName", StringType(), values=first_names, random=True)  # Generate column data at random or from repeatable seed values
        .withColumn("LastName", StringType(), values=last_names, random=True)  # Generate column data at random or from repeatable seed values
        .withColumn("Email", StringType(), expr="concat('customer_', id, '@example.com')")  # Use SQL based expressions to control or augment column generation
    )
    customers_df = customer_spec.build()
    return customers_df

def generate_products(no_of_rows):
    product_spec = (
        dg.DataGenerator(spark, name="products", rows=no_of_rows, partitions=4)  # Specify number of rows to generate, Specify number of Spark partitions to distribute data generation across
        .withIdOutput()
        .withColumn("ProductID", IntegerType(), expr="id")  # Generate column data from one or more seed columns
        .withColumn("ProductName", StringType(), values=["ProductA", "ProductB", "ProductC", "ProductD", "ProductE"], random=True)  # Generate column data at random or from repeatable seed values
        .withColumn("Price", FloatType(), minValue=5.0, maxValue=500.0, random=True)  # Specify numeric, time, and date ranges for columns
        .withColumn("Category", StringType(), values=["Category1", "Category2", "Category3"], random=True)  # Generate column data at random or from repeatable seed values
    )
    products_df = product_spec.build()
    return products_df

def generate_purchase_history(no_of_rows, customers_df, products_df):
    customer_ids = customers_df.select("CustomerID").rdd.flatMap(lambda x: x).collect()
    product_ids = products_df.select("ProductID").rdd.flatMap(lambda x: x).collect()
    
    purchase_history_spec = (
        dg.DataGenerator(spark, name="purchase_history", rows=no_of_rows, partitions=4)  # Specify number of rows to generate, Specify number of Spark partitions to distribute data generation across
        .withIdOutput()
        .withColumn("CustomerID", IntegerType(), values=customer_ids, random=True)  # Generate column data at random or from repeatable seed values
        .withColumn("ProductID", IntegerType(), values=product_ids, random=True)  # Generate column data at random or from repeatable seed values
        .withColumn("PurchaseDate", DateType(), expr="current_date()")  # Use SQL based expressions to control or augment column generation
        .withColumn("Quantity", IntegerType(), minValue=1, maxValue=10, random=True)  # Specify numeric, time, and date ranges for columns
    )
    purchase_history_df = purchase_history_spec.build()
    return purchase_history_df

no_of_rows = 2000  # Specify number of rows to generate

# Generate data
customers_df = generate_customers(no_of_rows)
products_df = generate_products(no_of_rows)
purchase_history_df = generate_purchase_history(no_of_rows, customers_df, products_df)

# Display the dataframes
print("Customers DataFrame:")
display(customers_df)

print("Products DataFrame:")
display(products_df)

print("PurchaseHistory DataFrame:")
display(purchase_history_df)

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import IntegerType, StringType, FloatType
from faker import Faker
import numpy as np

fake = Faker()

# Generate fake names
def generate_fake_names(no_of_rows):
    first_names = [fake.first_name() for _ in range(no_of_rows)]
    last_names = [fake.last_name() for _ in range(no_of_rows)]
    return first_names, last_names

# Generate customers DataFrame with age column
def generate_customers(no_of_rows):
    first_names, last_names = generate_fake_names(no_of_rows)
    
    # Assume mean age is 35 and standard deviation is 10
    mean_age = 35
    stddev_age = 10
    ages = np.random.normal(loc=mean_age, scale=stddev_age, size=no_of_rows).astype(int)
    
    customer_spec = (
        dg.DataGenerator(spark, name="customers", rows=no_of_rows, partitions=4)
        .withIdOutput()  # Specify number of rows to generate, Specify number of Spark partitions to distribute data generation across
        .withColumn("CustomerID", IntegerType(), expr="id")# Generate column data from one or more seed columns
        .withColumn("FirstName", StringType(), values=first_names, random=True) # Generate column data at random or from repeatable seed values
        .withColumn("LastName", StringType(), values=last_names, random=True)# Generate column data at random or from repeatable seed values
        .withColumn("Email", StringType(), expr="concat('customer_', id, '@example.com')")# Use SQL based expressions to control or augment column generation
        .withColumn("Age", IntegerType(), values=ages, random=True)  # Add age column with normal distribution
    )
    customers_df = customer_spec.build()
    return customers_df

# Generate products DataFrame
def generate_products(no_of_rows):
    product_spec = (
        dg.DataGenerator(spark, name="products", rows=no_of_rows, partitions=4)
        .withIdOutput()# Specify number of rows to generate, Specify number of Spark partitions to distribute data generation across
        .withColumn("ProductID", IntegerType(), expr="id")# Generate column data from one or more seed columns
        .withColumn("ProductName", StringType(), values=["ProductA", "ProductB", "ProductC", "ProductD", "ProductE"], random=True)# Generate column data at random or from repeatable seed values
        .withColumn("Price", FloatType(), minValue=5.0, maxValue=500.0, random=True)# Specify numeric, time, and date ranges for columns
        .withColumn("Category", StringType(), values=["Category1", "Category2", "Category3"], random=True) # Generate column data at random or from repeatable seed values
    )
    products_df = product_spec.build()
    return products_df

# Generate purchase_history DataFrame
def generate_purchase_history(no_of_rows, customers_df, products_df):
    customer_ids = customers_df.select("CustomerID").rdd.flatMap(lambda x: x).collect()
    product_ids = products_df.select("ProductID").rdd.flatMap(lambda x: x).collect()
    
    purchase_history_spec = (
        dg.DataGenerator(spark, name="purchase_history", rows=no_of_rows, partitions=4)
        .withIdOutput()
        .withColumn("CustomerID", IntegerType(), values=customer_ids, random=True)
        .withColumn("ProductID", IntegerType(), values=product_ids, random=True)
        .withColumn("Quantity", IntegerType(), minValue=1, maxValue=10, random=True)
    )# Specify numeric, time, and date ranges for columns
    purchase_history_df = purchase_history_spec.build()
    return purchase_history_df

no_of_rows = 100  # Specify number of rows to generate

# Generate data
customers_df = generate_customers(no_of_rows)
products_df = generate_products(no_of_rows)
purchase_history_df = generate_purchase_history(no_of_rows, customers_df, products_df)

# Join tables to create a single DataFrame with all specified columns
fact_table_df = (
    purchase_history_df
    .join(customers_df, on="CustomerID", how="inner")
    .join(products_df, on="ProductID", how="inner")
)

# Select all specified columns
fact_table_df = fact_table_df.select(
    customers_df["CustomerID"].alias("CustomerID"),
    customers_df["FirstName"],
    customers_df["LastName"],
    customers_df["Email"],
    customers_df["Age"],
    products_df["ProductID"].alias("ProductID"),
    products_df["ProductName"],
    products_df["Price"],
    products_df["Category"],
    purchase_history_df["CustomerID"].alias("PH_CustomerID"),
    purchase_history_df["ProductID"].alias("PH_ProductID"),
    purchase_history_df["Quantity"]
)

# Display the DataFrame
fact_table_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##code generation from existing schema

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from faker import Faker
#Support for code generation from existing schema or Spark dataframe to synthesize data

# Define the schema for the DataFrame
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", StringType(), True),
    StructField("City", StringType(), True)
])

# Create an empty DataFrame with the specified schema
df = spark.createDataFrame([], schema)

# Create a Faker instance
fake = Faker()

# Generate synthetic data using Faker
num_rows = 100
synthetic_data = []
for _ in range(num_rows):
    name = fake.name()
    age = fake.random_int(min=18, max=80)
    city = fake.city()
    synthetic_data.append((name, str(age), city))

# Convert synthetic data to a DataFrame
synthetic_df = spark.createDataFrame(synthetic_data, schema)

# Show the synthetic data DataFrame
synthetic_df.display()




# COMMAND ----------

# MAGIC %md
# MAGIC ##formatting on string columns

# COMMAND ----------


# Define the schema for the DataFrame
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Description", StringType(), True)  # New column for description
])

# Create an empty DataFrame with the specified schema
df = spark.createDataFrame([], schema)

# Create a Faker instance
fake = Faker()

# Generate synthetic data using Faker with template-based text generation
num_rows = 100
synthetic_data = []
for _ in range(num_rows):
    name = fake.name()
    age = fake.random_int(min=18, max=80)
    city = fake.city()
    
    # Generate a description template based on name, age, and city
    description = f"{name} is {age} years old and lives in {city}."
    
    synthetic_data.append((name, str(age), city, description))

# Convert synthetic data to a DataFrame
synthetic_df = spark.createDataFrame(synthetic_data, schema)

# Show the synthetic data DataFrame
synthetic_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##Script Spark SQL table creation statement for dataset

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import StructType, StructField,  StringType

partitions_requested = 8
data_rows = 1000

spark.sql("""Create table if not exists test_vehicle_data(
                name string, 
                serial_number string, 
                license_plate string, 
                email string
                ) using Delta""")

table_schema = spark.table("test_vehicle_data").schema

print(table_schema)
  
dataspec = (dg.DataGenerator(spark, rows=100000, partitions=8)
            .withSchema(table_schema))

dataspec = (
    dataspec.withColumnSpec("name", percentNulls=0.01, template=r"\\w \\w|\\w a. \\w")#template-based text generation
    .withColumnSpec(
        "serial_number", minValue=10, maxValue=100, prefix="dr", random=True
    )
    .withColumnSpec("email", template=r"\\w.\\w@\\w.com")
    .withColumnSpec("license_plate", template=r"\\n-\\n")
)
df1 = dataspec.build()
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##dynamic code generation

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import IntegerType, StringType, FloatType, DateType
from faker import Faker
import numpy as np
from datetime import datetime, timedelta


fake = Faker()

# Helper functions
def generate_fake_names(no_of_rows):
    first_names = [fake.first_name() for _ in range(no_of_rows)]
    last_names = [fake.last_name() for _ in range(no_of_rows)]
    return first_names, last_names

def generate_descriptions(first_names, last_names):
    return [f"Customer {first_name} {last_name} is a valued client." for first_name, last_name in zip(first_names, last_names)]

def generate_ages(no_of_rows, mean_age=35, stddev_age=10):
    return np.random.normal(loc=mean_age, scale=stddev_age, size=no_of_rows).astype(int)

def generate_prices(no_of_rows, min_price=5.0, max_price=500.0):
    return np.random.uniform(low=min_price, high=max_price, size=no_of_rows)

def generate_countries(no_of_rows):
    return [fake.country() for _ in range(no_of_rows)]

def generate_quantities(no_of_rows, min_quantity=1, max_quantity=100):
    return np.random.randint(low=min_quantity, high=max_quantity, size=no_of_rows)

def generate_dates(no_of_rows, start_date, end_date):
    start_time = datetime.strptime(start_date, "%Y-%m-%d")
    end_time = datetime.strptime(end_date, "%Y-%m-%d")
    return [(start_time + timedelta(days=np.random.randint(0, (end_time - start_time).days))).date() for _ in range(no_of_rows)]

# Main function to generate data
def generate_data(table_name, no_of_rows, column_specs):
    # Generate data based on specs
    first_names, last_names = generate_fake_names(no_of_rows)
    data_generators = {
        "first_names": first_names,
        "last_names": last_names,
        "descriptions": generate_descriptions(first_names, last_names),
        "ages": generate_ages(no_of_rows),
        "prices": generate_prices(no_of_rows),
        "countries": generate_countries(no_of_rows),
        "quantities": generate_quantities(no_of_rows),
        "dates": generate_dates(no_of_rows, "2020-01-01", "2023-12-31"),
        "email": [fake.email() for _ in range(no_of_rows)]
    }

    customer_spec = dg.DataGenerator(spark, name=table_name, rows=no_of_rows, partitions=4).withIdOutput()
    
    # Adding columns based on specs
    for col_name, col_type, values_func in column_specs:
        if values_func == "id":
            customer_spec = customer_spec.withColumn(col_name, col_type, expr="id")
        elif values_func == "email_expr":
            customer_spec = customer_spec.withColumn(col_name, col_type, expr="concat('customer_', id, '@example.com')")
        else:
            values = data_generators[values_func]
            customer_spec = customer_spec.withColumn(col_name, col_type, values=values, random=True)

    return customer_spec.build()

# Example usage:

no_of_rows = 20  # Specify number of rows to generate

column_specs = [
    ("CustomerID", IntegerType(), "id"),  # Generate column data from one or more seed columns
    ("FirstName", StringType(), "first_names"),
    ("LastName", StringType(), "last_names"),
    ("Email", StringType(), "email_expr"),  # Use SQL based expressions to control or augment column generation
    ("Age", IntegerType(), "ages"),
    ("Price", FloatType(), "prices"),
    ("Description", StringType(), "descriptions"),
    ("Country", StringType(), "countries"),
    ("Quantity", IntegerType(), "quantities"),
    ("Date", DateType(), "dates")
]

# Generate DataFrame
customers_df = generate_data("customers", no_of_rows, column_specs)

# Display the Customers DataFrame
print("Customers DataFrame:")
customers_df.display()

