# Databricks notebook source
# DBTITLE 1,Imports
# Import SparkSession
from pyspark.sql import SparkSession

# COMMAND ----------

# DBTITLE 1,Create Spark Session
# Create a SparkSession object
spark = SparkSession.builder \
    .appName("CreatePysparkDataFrame") \
    .master("local") \
    .getOrCreate()


# COMMAND ----------

# DBTITLE 1,Using Tuples No Column Names
# Declare the Data - List of tuples
data = [
  (1,'£100.98', '23', '10/12/2024')
  , (2, '£2,098.98', '99', '22/05/2024')
  , (3, '-£2.56', '700', '01/09/2024')
]

# Initialise the pyspark data frame
df = spark.createDataFrame(data)

# Display the data frame
df.show()


# COMMAND ----------

# DBTITLE 1,Empty Data Frame
# DDL String
schema = "TransactionID: int, TransactionAmount: string, CustomerID: string, TransactionDate: string"

# Initialise Pyspark Data Frame
df = spark.createDataFrame(data = [], schema=schema)
df.show()

# COMMAND ----------

# DBTITLE 1,Using Tuples With Column Headers
# Declare the Data - List of tuples
data = [
  (1,'£100.98', '23', '10/12/2024')
  , (2, '£2,098.98', '99', '22/05/2024')
  , (3, '-£2.56', '700', '01/09/2024')
]

# Declare Column Names
columns = ['TransactionID', 'TransactionAmount','CustomerID','TransactionDate']

# Initialise Pyspark Data Frame
df = spark.createDataFrame(data = data, schema = columns)
df.show()

# COMMAND ----------

# DBTITLE 1,Using Dictionary
# Create list of dictionaries
data = [
  {'TransactionID':1, 'TransactionAmount':'£100.98', 'CustomerID':'23' , 'TransactionDate':'10/12/2024'}
  , {'TransactionID':2, 'TransactionAmount':'£2,098.98', 'CustomerID':'99' , 'TransactionDate':'22/05/2024'}
  , {'TransactionID':3, 'TransactionAmount':'-£2.56', 'CustomerID':'700' , 'TransactionDate':'01/09/2024'}
]

# Initialise Pyspark Data Frame
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

# DBTITLE 1,With Schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
   StructField("TransactionID", IntegerType(), True),
   StructField("TransactionAmount", StringType(), True),
   StructField("CustomerId", StringType(), True),
   StructField("TransactionDate", StringType(), True),
  ])

# Declare the Data - List of tuples
data = [
  (1,'£100.98', '23', '10/12/2024')
  , (2, '£2,098.98', '99', '22/05/2024')
  , (3, '-£2.56', '700', '01/09/2024')
]

# Initialise Pyspark Data Frame
df = spark.createDataFrame(data = data, schema = schema)
df.show()

# COMMAND ----------

# DBTITLE 1,DDL String
# Declare the Data - List of tuples
data = [
  (1,'£100.98', '23', '10/12/2024')
  , (2, '£2,098.98', '99', '22/05/2024')
  , (3, '-£2.56', '700', '01/09/2024')
]

# DDL String
schema = "TransactionID: int, TransactionAmount: string, CustomerID: string, TransactionDate: string"

# Initialise Pyspark Data Frame
df = spark.createDataFrame(data = data, schema=schema)
df.show()


# COMMAND ----------

# DBTITLE 1,Row Objects
from pyspark.sql import Row

Transaction = Row('TransactionID','TransactionAmount','CustomerID','TransactionDate')

data = [
    Transaction(1,'£100.98', '23', '10/12/2024')
  , Transaction(2, '£2,098.98', '99', '22/05/2024')
  , Transaction(3, '-£2.56', '700', '01/09/2024')
]

# Initialise Pyspark Data Frame
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

# DBTITLE 1,Using Pandas
import pandas as pd 

# Declare the Data - List of tuples
data = [
  [1,'£100.98', '23', '10/12/2024']
  , [2, '£2,098.98', '99', '22/05/2024']
  , [3, '-£2.56', '700', '01/09/2024']
]

# Declare column names
columns = ['TransactionID', 'TransactionAmount','CustomerID','TransactionDate']

# Create Pandas Data Frame 
pd_df = pd.DataFrame(data,columns=columns)

# Create Pyspark Data Frame
df = spark.createDataFrame(pd_df)
df.show()
