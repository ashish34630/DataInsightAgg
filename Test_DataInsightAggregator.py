# Databricks notebook source
# Notebook Integration: Executes the DataInsightAggregator notebook, incorporating its data processing functions and tests into this workflow
# MAGIC %run ./DataInsightAggregator

# COMMAND ----------

from pyspark.sql import DataFrame
from typing import List, Dict, Any

# Mock data for testing
mock_orders_data: List[Dict[str, Any]] = [
    {"Customer ID": "C1", "Discount": 0.1, "Order Date": "01/01/2020", "Order ID": "O1", "Price": 100.0, "Product ID": "P1", "Profit": 20.0, "Quantity": 2, "Row ID": 1, "Ship Date": "03/01/2020", "Ship Mode": "First Class"},
]

mock_products_data: List[Dict[str, Any]] = [
    {"Product ID": "P1", "Category": "Technology", "Sub-Category": "Accessories", "Product Name": "Product 1", "State": "New York", "Price per product": 50.0},
]

mock_customers_data: List[Dict[str, Any]] = [
    {"Customer ID": "C1", "Customer Name": "Customer 1", "email": "email@example.com", "phone": "1234567890", "address": "Address 1", "Segment": "Segment 1", "Country": "USA", "City": "New York", "State": "NY", "Postal Code": "10001", "Region": "East"},
]

def create_mock_df(data: List[Dict[str, Any]], schema: StructType) -> DataFrame:
    """
    Create a Spark DataFrame from mock data.
    
    Parameters:
    - data: List of dictionaries representing rows of data.
    - schema: Spark StructType schema defining the DataFrame structure.
    
    Returns:
    - DataFrame: A Spark DataFrame created from the provided data and schema.
    """
    return spark.createDataFrame(data, schema)

def test_read_data() -> None:
    """
    Test the creation of DataFrames from mock data.
    """
    df_orders_mock = create_mock_df(mock_orders_data, orders_schema)
    df_products_mock = create_mock_df(mock_products_data, products_schema)
    df_customers_mock = create_mock_df(mock_customers_data, customers_schema)
    
    assert df_orders_mock.count() == len(mock_orders_data), "Orders DataFrame row count does not match mock data count"
    assert df_products_mock.count() == len(mock_products_data), "Products DataFrame row count does not match mock data count"
    assert df_customers_mock.count() == len(mock_customers_data), "Customers DataFrame row count does not match mock data count"

def test_process_data() -> None:
    """
    Test the data processing logic.
    """
    # Create mock DataFrames and replace temp views for testing
    df_orders_mock = create_mock_df(mock_orders_data, orders_schema)
    df_products_mock = create_mock_df(mock_products_data, products_schema)
    df_customers_mock = create_mock_df(mock_customers_data, customers_schema)
    
    df_orders_mock.createOrReplaceTempView("raw_orders")
    df_products_mock.createOrReplaceTempView("raw_products")
    df_customers_mock.createOrReplaceTempView("raw_customers")
    
    process_data()
    
    result_df = spark.sql("SELECT * FROM enriched_orders")
    assert result_df.count() > 0, "No data in enriched_orders after processing"

def test_enriched_data_columns() -> None:
    """
    Test the schema of the enriched data DataFrame.
    """
    expected_columns = sorted(["OrderID", "OrderDate", "Profit", "CustomerName", "Country", "Category", "SubCategory"])
    enriched_df = spark.sql("SELECT * FROM enriched_orders")
    enriched_columns = sorted(enriched_df.columns)

    assert enriched_columns == expected_columns, f"Enriched data columns mismatch. Expected: {expected_columns}, Found: {enriched_columns}"

def test_aggregated_table_columns() -> None:
    """
    Test the schema of the aggregated table DataFrame.
    """
    expected_columns = sorted(["Year", "Category", "SubCategory", "CustomerName", "TotalProfit"])
    aggregated_df = spark.sql("SELECT * FROM aggregate_table")
    aggregated_columns = sorted(aggregated_df.columns)

    assert aggregated_columns == expected_columns, f"Aggregated table columns mismatch. Expected: {expected_columns}, Found: {aggregated_columns}"

def cleanup() -> None:
    """
    Cleanup temporary views created during tests.
    """
    spark.catalog.dropTempView("raw_orders")
    spark.catalog.dropTempView("raw_products")
    spark.catalog.dropTempView("raw_customers")
    spark.catalog.dropTempView("enriched_orders")
    spark.catalog.dropTempView("aggregate_table")


def run_all_tests() -> None:
    """
    Execute all test functions and cleanup after.
    """
    try:
        test_read_data()
        test_process_data()
        test_enriched_data_columns()
        test_aggregated_table_columns()
    finally:
        cleanup()  # Ensure cleanup happens even if tests fail


# Execute all tests
run_all_tests()
