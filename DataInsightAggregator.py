# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import unix_timestamp, from_unixtime, to_date, round as spark_round

# Initialize Spark session once at the beginning
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Define schemas for different data sources
orders_schema = StructType([
    StructField("Customer ID", StringType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Order Date", StringType(), True),
    StructField("Order ID", StringType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Product ID", StringType(), True),
    StructField("Profit", DoubleType(), True),
    StructField("Quantity", LongType(), True),
    StructField("Row ID", LongType(), True),
    StructField("Ship Date", StringType(), True),
    StructField("Ship Mode", StringType(), True)
])

products_schema = StructType([
    StructField("Product ID", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Sub-Category", StringType(), True),
    StructField("Product Name", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Price per product", DoubleType(), True)
])

customers_schema = StructType([
    StructField("Customer ID", StringType(), True),
    StructField("Customer Name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("Segment", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Postal Code", StringType(), True),
    StructField("Region", StringType(), True)
])

def read_data() -> tuple[DataFrame, DataFrame, DataFrame]:
    """Read data sources with error handling."""
    try:
        df_orders = spark.read.option("multiline", "true") \
                              .option("mode", "PERMISSIVE") \
                              .option("columnNameOfCorruptRecord", "_corrupt_record") \
                              .schema(orders_schema) \
                              .json("/FileStore/Order_1_.json")

        df_products = spark.read.option("header", "true") \
                                .option("mode", "PERMISSIVE") \
                                .schema(products_schema) \
                                .csv("/FileStore/Product_1_.csv")

        df_customers = spark.read.format("com.crealytics.spark.excel") \
                                  .option("mode", "PERMISSIVE") \
                                  .option("columnNameOfCorruptRecord", "_corrupt_record") \
                                  .option("header", "true") \
                                  .schema(customers_schema) \
                                  .load("/FileStore/Customer_1_.xlsx")

        return df_orders, df_products, df_customers
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        exit()

def process_data()->None:
    """Process data."""
    # Enrich and create aggregated tables via SQL operations
    spark.sql("""
    SELECT 
        o.`Order ID` AS OrderID,
        to_date(from_unixtime(unix_timestamp(o.`Order Date`, 'dd/MM/yyyy'))) AS OrderDate, 
        ROUND(o.Profit, 2) AS Profit, 
        c.`Customer Name` AS CustomerName, 
        c.Country, 
        p.Category, 
        p.`Sub-Category` AS SubCategory
    FROM raw_orders o
    JOIN raw_customers c ON o.`Customer ID` = c.`Customer ID`
    JOIN raw_products p ON o.`Product ID` = p.`Product ID`
    """).createOrReplaceTempView("enriched_orders")

    # Aggregated queries
    spark.sql("""
    SELECT 
        YEAR(OrderDate) AS Year, 
        Category, 
        SubCategory, 
        CustomerName,
        SUM(Profit) AS TotalProfit
    FROM enriched_orders
    GROUP BY YEAR(OrderDate), Category, SubCategory, CustomerName
    """).createOrReplaceTempView("aggregate_table")

def display_results()->None:
    """Display results."""
    spark.sql("SELECT Year, SUM(TotalProfit) AS Profit FROM aggregate_table GROUP BY Year").show()
    spark.sql("SELECT Year, Category, SUM(TotalProfit) AS Profit FROM aggregate_table GROUP BY Year, Category").show()
    spark.sql("SELECT CustomerName, SUM(TotalProfit) AS Profit FROM aggregate_table GROUP BY CustomerName").show()
    spark.sql("SELECT CustomerName, Year, SUM(TotalProfit) AS Profit FROM aggregate_table GROUP BY CustomerName, Year").show()


def main():
    df_orders, df_products, df_customers = read_data()

    # Create temporary views
    df_orders.createOrReplaceTempView("raw_orders")
    df_customers.createOrReplaceTempView("raw_customers")
    df_products.createOrReplaceTempView("raw_products")

    process_data()
    display_results()


