# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import datetime

# Initialize SparkSession
spark = SparkSession.builder.appName("SCD2Example").getOrCreate()

# Create the schema
schema = StructType([
    StructField("EmpID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Dept", StringType(), True),
    StructField("Salary", DoubleType(), True),
    StructField("City", StringType(), True),
    StructField("Start_Date", DateType(), True),
    StructField("End_Date", DateType(), True),
    StructField("Is_Current", IntegerType(), True)
])

# Convert the date strings to actual date objects in Python
initial_data = [
    (1, 'Alice', 'HR', 50000.0, 'New York', datetime.strptime('2023-01-01', '%Y-%m-%d').date(), None, 1),
    (2, 'Bob', 'Sales', 60000.0, 'London', datetime.strptime('2023-01-01', '%Y-%m-%d').date(), None, 1)
]

# Create DataFrame using the schema
df_existing = spark.createDataFrame(initial_data, schema=schema)

# Show the DataFrame
df_existing.show()


# COMMAND ----------

new_data = [
    (1, 'Alice', 'HR', 52000, 'New York', '2023-06-01'),
    (3, 'Charlie', 'IT', 70000, 'Berlin', '2023-06-01')
]

new_columns = ['EmpID', 'Name', 'Dept', 'Salary', 'City', 'Start_Date']
df_new = spark.createDataFrame(new_data, new_columns)

# COMMAND ----------

df_new.display()

# COMMAND ----------

df_to_update = df_existing.alias("old").join(
    df_new.alias("new"),
    on="EmpID",
    how="inner"
).where(
    (col("old.Salary") != col("new.Salary"))  # If any field you want to track has changed
)
df_to_update.display()

# COMMAND ----------

df_existing_updated = df_existing.join(
    df_to_update.select("EmpID"),
    on="EmpID",
    how="left"
)

# COMMAND ----------

df_existing_updated.display()

# COMMAND ----------

df_existing_updated = df_existing.join(
    df_to_update.select("EmpID"),
    on="EmpID",
    how="left"
).withColumn(
    "End_Date",
    when(col("EmpID").isNotNull(), current_date()).otherwise(col("End_Date"))
).withColumn(
    "Is_Current",
    when(col("EmpID").isNotNull(), lit(0)).otherwise(col("Is_Current"))
)

# COMMAND ----------

df_existing_updated.display()

# COMMAND ----------

df_new_records = df_new.withColumn("End_Date", lit(None).cast("date")) \
                       .withColumn("Is_Current", lit(1))
                       


# COMMAND ----------

df_new_records.display()

# COMMAND ----------

df_final = df_existing_updated.unionByName(df_new_records)


# COMMAND ----------

df_final.display()

# COMMAND ----------


