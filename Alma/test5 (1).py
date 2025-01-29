# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, concat_ws, to_date, lit, when, current_timestamp, rlike
import time


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS test_silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS test_gold;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS test_silver.customer_table (
# MAGIC     CustomerID INTEGER,
# MAGIC     CustomerName STRING,
# MAGIC     City STRING,
# MAGIC     JoinDate DATE,
# MAGIC     Email STRING,
# MAGIC     LoadTimestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/tmp/silver/customer';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS test_gold.customer_table (
# MAGIC     CustomerID INTEGER,
# MAGIC     CustomerName STRING,
# MAGIC     City STRING,
# MAGIC     JoinDate DATE,
# MAGIC     Email STRING,
# MAGIC     LoadTimestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/tmp/gold/customer';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS test_silver.sales_table (
# MAGIC     SaleID STRING,
# MAGIC     CustomerID INTEGER,
# MAGIC     ProductID INTEGER,
# MAGIC     ProductName STRING,
# MAGIC     Quantity INTEGER,
# MAGIC     Price DOUBLE,
# MAGIC     TotalPrice DOUBLE,
# MAGIC     SaleDate STRING,
# MAGIC     PaymentMethod STRING,
# MAGIC     PaymentDescription STRING,
# MAGIC     LoadTimestamp TIMESTAMP 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/tmp/silver/sales';
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS test_gold.sales_table (
# MAGIC     SaleID STRING,
# MAGIC     CustomerID INTEGER,
# MAGIC     ProductID INTEGER,
# MAGIC     ProductName STRING,
# MAGIC     Quantity INTEGER,
# MAGIC     Price DOUBLE,
# MAGIC     TotalPrice DOUBLE,
# MAGIC     SaleDate DATE,
# MAGIC     PaymentMethod STRING,
# MAGIC     PaymentDescription STRING,
# MAGIC     LoadTimestamp TIMESTAMP 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/tmp/gold/sales';

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions

# COMMAND ----------

def load_bronze_data(source_file, bronze_folder):
    """
    Loads data from a CSV file without a header and saves it as a CSV file in the bronze folder.

    Parameters:
    source_file (str): The path to the source CSV file.
    bronze_folder (str): The path to the bronze folder where the CSV file will be saved.
    """
    dbutils.fs.cp(source_file, bronze_folder)

# COMMAND ----------

def load_silver_customer_data(bronze_file, silver_folder, archive_folder):
    """
    Loads customer data from a CSV file, adds a timestamp column, and saves it as a Delta table.

    Parameters:
    bronze_file (str): The path to the bronze_file CSV file.
    silver_folder (str): The path to the silver folder where the Delta table will be saved.
    """
    # Read the CSV file into a DataFrame
    customer_df = spark.read.csv(bronze_file, header=True, inferSchema=True)

    # Add a timestamp column
    customer_df = customer_df.withColumn("timestamp", current_timestamp())

    # Save the DataFrame as a Delta table
    customer_df.write.format("delta").mode("overwrite").save(silver_folder)

    # If the Delta table already exists, delete it
    dbutils.fs.mv(bronze_file, archive_folder,recurse=True)




# COMMAND ----------


def clean_silver_customer_data(silver_folder, log_folder):
    """
    Cleans customer data by removing duplicate and invalid email records,
    and returns the cleaned DataFrame.

    Parameters:
    silver_folder (str): The path to the silver layer data.
    log_folder (str): The path to the log files.

    Returns:
    DataFrame: The cleaned customer DataFrame.
    """
    # Read the Delta file
    customer_df = spark.read.format("delta").load(silver_folder)

    # Identify duplicate records
    duplicate_records_df = customer_df.groupBy("CustomerID").count().filter(col("count") > 1).select("CustomerID")


    # Identify invalid email records
    invalid_email_df = customer_df.filter(~col("Email").rlike(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")).select("CustomerID", "CustomerName", "City", "JoinDate", "Email")
    
    #Identify null email addresses
    null_email_df = customer_df.filter(col("Email").isNull()).select("CustomerID", "CustomerName", "City", "JoinDate", "Email")

    # Identify records affected by the cleaning process
    affected_records_df = customer_df \
        .join(duplicate_records_df, "CustomerID", "left_semi") \
        .select("CustomerID", "CustomerName", "City", "JoinDate", "Email")

    # Add problem column
    affected_records_df = affected_records_df.withColumn("problem", lit("Duplicate"))

    # Add invalid  null email addresses
    null_email_with_problem_df = null_email_df.withColumn("problem", lit("Null email"))
    affected_records_df = affected_records_df.unionByName(null_email_with_problem_df)

    # Add invalid email records
    invalid_email_with_problem_df = invalid_email_df.withColumn("problem", lit("Invalid email"))
    affected_records_df = affected_records_df.unionByName(invalid_email_with_problem_df)

    # Add file name and timestamp columns
    affected_records_df = affected_records_df.withColumn("file_name", lit(log_folder)).withColumn("timestamp", current_timestamp())

    # Write affected records to a file
    affected_records_df.write.csv(log_folder, header=True, mode="overwrite")


    # Filter out duplicate and invalid email records for the cleaned DataFrame
    cleaned_customer_df = customer_df \
        .join(duplicate_records_df, "CustomerID", "left_anti") \
        .filter(col("Email").rlike(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")) \
        .withColumn("timestamp", current_timestamp())

    return cleaned_customer_df


# COMMAND ----------


def load_silver_sales_data(bronze_file, silver_folder, archive_folder, row_to_check):
    """
    Loads sales data from a CSV file, cleans the data, and saves it as a Delta table.

    Parameters:
    source_file (str): The path to the source CSV file.
    silver_folder (str): The path to the silver folder where the Delta table will be saved.
    """

    # Read the CSV file without a header
    sales_df = spark.read.option("header", "false").csv(bronze_file)

    # Header string and split it into a list
    columns = row_to_check.split(",")

    # Concatenate columns using concat_ws
    sales_df = sales_df.withColumn("concat_col", concat_ws(",", *sales_df.columns))

    # Filter out the header row from the DataFrame
    df_cleaned = sales_df.filter(col("concat_col") != row_to_check).drop("concat_col")

    # Set the columns as the header
    df_cleaned = df_cleaned.toDF(*columns)

    # Define the new schema
    silver_sales_schema = StructType([
        StructField("SaleID", StringType(), True),
        StructField("CustomerID", IntegerType(), True),
        StructField("ProductID", IntegerType(), True),
        StructField("ProductName", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Price", DoubleType(), True),
        StructField("TotalPrice", DoubleType(), True),
        StructField("SaleDate", StringType(), True),
        StructField("PaymentMethod", StringType(), True),
        StructField("PaymentDescription", StringType(), True)
    ])

    # Convert data types and apply the schema
    df_cleaned = df_cleaned \
        .withColumn("CustomerID", col("CustomerID").cast(IntegerType())) \
        .withColumn("ProductID", col("ProductID").cast(IntegerType())) \
        .withColumn("Quantity", col("Quantity").cast(IntegerType())) \
        .withColumn("Price", col("Price").cast(DoubleType())) \
        .withColumn("TotalPrice", col("TotalPrice").cast(DoubleType())) 
        #.withColumn("SaleDate", to_date(col("SaleDate"), "yyyy-MM-dd"))

    # Create DataFrame with the new schema
    df_cleaned = spark.createDataFrame(df_cleaned.rdd, schema=silver_sales_schema)

    # Add a timestamp column
    df_cleaned = df_cleaned.withColumn("timestamp", current_timestamp())

    # Save the DataFrame as a Delta table
    df_cleaned.write.format("delta").mode("overwrite").save(silver_folder)

    # If the Delta table already exists, delete it
    dbutils.fs.mv(bronze_file, archive_folder,recurse=True)


# COMMAND ----------


def clean_silver_sales_data(silver_sales_folder, silver_customer_folder, log_folder):
    """
    Cleans sales data by removing duplicate SaleID records, fixing invalid date formats,
    handling null PaymentMethod values, and checking CustomerID validity.
    Logs affected records and returns the cleaned DataFrame.

    Parameters:
    silver_sales_folder (str): The path to the silver layer sales data.
    silver_customer_folder (str): The path to the silver layer customer data.
    log_folder (str): The path to the log files.

    Returns:
    DataFrame: The cleaned sales DataFrame.
    """

    # Read the data
    sales_df = spark.read.format("delta").load(silver_sales_folder)
    customer_df = spark.read.format("delta").load(silver_customer_folder)

    # Check for and remove duplicate SaleID records
    duplicate_sales_df = sales_df.groupBy("SaleID").count().filter(col("count") > 1).select("SaleID")

    # Identify affected records
    affected_records_df = sales_df \
        .join(duplicate_sales_df, "SaleID", "left_semi") \
        .select("SaleID", "CustomerID", "ProductID", "ProductName", "Quantity", "Price", "TotalPrice", "SaleDate", "PaymentMethod", "PaymentDescription")

    # Add problem column
    affected_records_df = affected_records_df.withColumn("problem", lit("Duplicate"))

    # Check and fix invalid date formats
    invalid_date_df = sales_df.filter(~col("SaleDate").cast("date").isNotNull()).select("SaleID", "CustomerID", "ProductID", "ProductName", "Quantity", "Price", "TotalPrice", "SaleDate", "PaymentMethod", "PaymentDescription")

    # Mark invalid date format records
    invalid_date_with_problem_df = invalid_date_df.withColumn("problem", lit("Invalid date format"))
    affected_records_df = affected_records_df.unionByName(invalid_date_with_problem_df)

    # Handle null PaymentMethod values
    sales_df = sales_df.withColumn("PaymentMethod", when(col("PaymentMethod").isNull(), lit("NA")).otherwise(col("PaymentMethod")))

    # Check CustomerID validity against the customer table
    invalid_customer_df = sales_df.join(customer_df.select("CustomerID"), "CustomerID", "left_anti").select("SaleID", "CustomerID", "ProductID", "ProductName", "Quantity", "Price", "TotalPrice", "SaleDate", "PaymentMethod", "PaymentDescription")
    invalid_customer_with_problem_df = invalid_customer_df.withColumn("problem", lit("Invalid CustomerID"))

    # Add invalid CustomerID records to affected records
    affected_records_df = affected_records_df.unionByName(invalid_customer_with_problem_df)

    # Add file name and timestamp columns to affected records
    affected_records_df = affected_records_df.withColumn("file_name", lit(log_folder)).withColumn("timestamp", current_timestamp())

    # Write affected records to a file
    affected_records_df.write.csv(log_folder, header=True, mode="overwrite")

    # Filter out duplicate, invalid date format, and invalid CustomerID records for the cleaned DataFrame
    cleaned_sales_df = sales_df \
        .join(duplicate_sales_df, "SaleID", "left_anti") \
        .filter(col("SaleDate").cast("date").isNotNull()) \
        .join(customer_df.select("CustomerID"), "CustomerID", "inner") \
        .withColumn("timestamp", current_timestamp())

    #due to the date cleansing (2024-04.29), the date field is not a date type, so we need to cast it
    cleaned_sales_df = cleaned_sales_df.withColumn("SaleDate", col("SaleDate").cast("date"))

    # Select the required columns
    cleaned_sales_df = cleaned_sales_df.select(
    "SaleID", "CustomerID", "ProductID", "ProductName", "Quantity", "Price", "TotalPrice", 
    "SaleDate", "PaymentMethod", "PaymentDescription", "timestamp")

    return cleaned_sales_df



# COMMAND ----------

# MAGIC %md
# MAGIC ####Parameters

# COMMAND ----------


timestamp = time.strftime("%Y%m%d-%H%M%S")
customer_archive_folder = f"dbfs:/tmp/bronze/archive/customer_{timestamp}.csv/"
customer_source_file = "dbfs:/tmp/customer_data.csv"
customer_bronze_file = "dbfs:/tmp/bronze/customer/customer_data.csv"
customer_silver_folder = "dbfs:/tmp/silver/customer"
sales_archive_folder = f"dbfs:/tmp/bronze/archive/sales_{timestamp}.csv/"
sales_source_file = "dbfs:/tmp/sales_data.csv"
sales_bronze_file = "dbfs:/tmp/bronze/sales/sales_data.csv"
sales_silver_folder = "dbfs:/tmp/silver/sales"
customer_log_folder = "dbfs:/tmp/customer_data_cleansing_log/error_records.csv"
sales_log_folder = "dbfs:/tmp/sales_data_cleansing_log/error_records.csv"
row_to_check = "SaleID,CustomerID,ProductID,ProductName,Quantity,Price,TotalPrice,SaleDate,PaymentMethod,PaymentDescription"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Bronze loading

# COMMAND ----------


load_bronze_data(customer_source_file, customer_bronze_file)
load_bronze_data(sales_source_file, sales_bronze_file)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Silver loading

# COMMAND ----------

load_silver_customer_data(customer_bronze_file, customer_silver_folder,customer_archive_folder)
load_silver_sales_data(sales_bronze_file, sales_silver_folder,sales_archive_folder,row_to_check)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Silver data cleansing

# COMMAND ----------

cleaned_customer_df = clean_silver_customer_data(customer_silver_folder, customer_log_folder)
cleaned_sales_df = clean_silver_sales_data(sales_silver_folder, customer_silver_folder ,sales_log_folder)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Gold Loading

# COMMAND ----------

cleaned_customer_df.write.format("delta").mode("overwrite").save("dbfs:/tmp/gold/customer")
cleaned_sales_df.write.format("delta").mode("overwrite").save("dbfs:/tmp/gold/sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.city,
# MAGIC            s.CustomerID as CustomerID, 
# MAGIC            COUNT(DISTINCT s.ProductID) AS product_count,
# MAGIC            SUM(COALESCE(s.TotalPrice, 0)) AS total_amount
# MAGIC     FROM test_gold.sales_table as s
# MAGIC     INNER JOIN test_gold.customer_table as c ON s.CustomerID = c.CustomerID
# MAGIC     WHERE s.SaleDate >= DATE_SUB(CURRENT_DATE(), 90)
# MAGIC     GROUP BY c.city, s.CustomerID
# MAGIC     HAVING COUNT(DISTINCT s.ProductID) >= 3

# COMMAND ----------

# MAGIC %md
# MAGIC Task 3 Write a query that answers the following question: 
# MAGIC Which cities have at least 5 customers who purchased at least 3 different products in the last 90 days, and where the total --purchase value of these customers falls in the top 50%?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH recent_sales AS (
# MAGIC     SELECT c.city,
# MAGIC            s.CustomerID,
# MAGIC            COUNT(DISTINCT s.ProductID) AS product_count,
# MAGIC            SUM(COALESCE(s.TotalPrice, 0)) AS total_amount
# MAGIC     FROM test_gold.sales_table s
# MAGIC     JOIN test_gold.customer_table c ON s.CustomerID = c.CustomerID
# MAGIC     WHERE s.SaleDate >= DATE_SUB(CURRENT_DATE(), 90)
# MAGIC     GROUP BY c.city, s.CustomerID
# MAGIC     HAVING COUNT(DISTINCT s.ProductID) >= 3
# MAGIC ),
# MAGIC city_aggregates AS (
# MAGIC     SELECT city,
# MAGIC            COUNT(CustomerID) AS customer_count,
# MAGIC            SUM(total_amount) AS total_city_sales
# MAGIC     FROM recent_sales
# MAGIC     GROUP BY city
# MAGIC     HAVING COUNT(CustomerID) >= 5
# MAGIC ),
# MAGIC city_percentiles AS (
# MAGIC     SELECT city,
# MAGIC            NTILE(2) OVER (ORDER BY total_city_sales DESC) AS sales_percentile
# MAGIC     FROM city_aggregates
# MAGIC )
# MAGIC SELECT city
# MAGIC FROM city_percentiles
# MAGIC WHERE sales_percentile = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH recent_sales AS (
# MAGIC     SELECT c.city,
# MAGIC            s.CustomerID,
# MAGIC            COUNT(DISTINCT s.ProductID) AS product_count,
# MAGIC            SUM(COALESCE(s.TotalPrice, 0)) AS total_amount
# MAGIC     FROM test_silver.sales_table s
# MAGIC     JOIN test_silver.customer_table c ON s.CustomerID = c.CustomerID
# MAGIC     WHERE s.SaleDate >= DATE_SUB(CURRENT_DATE(), 90)
# MAGIC     GROUP BY c.city, s.CustomerID
# MAGIC     HAVING COUNT(DISTINCT s.ProductID) >= 3
# MAGIC ),
# MAGIC city_aggregates AS (
# MAGIC     SELECT city,
# MAGIC            COUNT(CustomerID) AS customer_count,
# MAGIC            SUM(total_amount) AS total_city_sales
# MAGIC     FROM recent_sales
# MAGIC     GROUP BY city
# MAGIC     HAVING COUNT(CustomerID) >= 5
# MAGIC ),
# MAGIC city_percentiles AS (
# MAGIC     SELECT city,
# MAGIC            NTILE(2) OVER (ORDER BY total_city_sales DESC) AS sales_percentile
# MAGIC     FROM city_aggregates
# MAGIC )
# MAGIC SELECT city
# MAGIC FROM city_percentiles
# MAGIC WHERE sales_percentile = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.city,
# MAGIC            s.CustomerID,
# MAGIC            COUNT(DISTINCT s.ProductID) AS product_count,
# MAGIC            SUM(COALESCE(s.TotalPrice, 0)) AS total_amount
# MAGIC     FROM test_gold.sales_table s
# MAGIC     JOIN test_gold.customer_table c ON s.CustomerID = c.CustomerID
# MAGIC     WHERE s.SaleDate >= DATE_SUB(CURRENT_DATE(), 90)
# MAGIC     GROUP BY c.city, s.CustomerID

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH recent_sales AS (
# MAGIC     SELECT c.city,
# MAGIC            s.CustomerID,
# MAGIC            COUNT(DISTINCT s.ProductID) AS product_count,
# MAGIC            SUM(COALESCE(s.TotalPrice, 0)) AS total_amount
# MAGIC     FROM test_gold.sales_table s
# MAGIC     JOIN test_gold.customer_table c ON s.CustomerID = c.CustomerID
# MAGIC     WHERE s.SaleDate >= DATE_SUB(CURRENT_DATE(), 90)
# MAGIC     GROUP BY c.city, s.CustomerID
# MAGIC     HAVING COUNT(DISTINCT s.ProductID) >= 3
# MAGIC ),
# MAGIC city_aggregates AS (
# MAGIC     SELECT city,
# MAGIC            COUNT(CustomerID) AS customer_count,
# MAGIC            SUM(total_amount) AS total_city_sales
# MAGIC     FROM recent_sales
# MAGIC     GROUP BY city
# MAGIC     HAVING COUNT(CustomerID) >= 5
# MAGIC     )
# MAGIC
# MAGIC select * from city_aggregates

# COMMAND ----------

# MAGIC %md
# MAGIC ####Log files

# COMMAND ----------

sales_df = spark.read.csv("dbfs:/tmp/customer_data_cleansing_log/error_records.csv", header=True, inferSchema=True)
sales_df.display()

# COMMAND ----------

sales_df = spark.read.csv("dbfs:/tmp/sales_data_cleansing_log/error_records.csv", header=True, inferSchema=True)
sales_df.display()

# COMMAND ----------

