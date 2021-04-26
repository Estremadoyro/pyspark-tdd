import findspark
findspark.init('/home/leonardo/spark-3.1.1-bin-hadoop2.7')
from pyspark.sql import SparkSession
#Creation of Spark Session 
spark = SparkSession \
    .builder \
    .appName("UNIVERSO INTERACCION TESTING") \
    .getOrCreate()

from pyspark.sql import functions as F
from pyspark.sql.functions import col

import pytest

# This method reads all parquet files sent as arguments and creates a Spark Dataframe for each one
# read_parquet_files(source1, source2, source3): returns a List of Spark Datagrames
# source1, source2, source 3: Parquet file name, should be String
@pytest.fixture
def read_parquet_files (source1="m_destipointeraccion", source2="h_interaccioncajero", source3= "M_UNIVERSOS_INTERACCION"):
  m_destipointeraccion = spark.read.parquet("./inputs caso practico/{}".format(source1))
  h_interaccioncajero = spark.read.parquet("./inputs caso practico/{}".format(source2))
  m_universos_interaccion = spark.read.parquet("{}".format(source3))
  dfs = [m_destipointeraccion, h_interaccioncajero,m_universos_interaccion]
  return dfs

# This method checks if a dataframe is empty 
# filter_empty_df (read_parquet_files): returns False if finds an empty Dataframe
# otherwise return True
# read_parquet_files: Method, should return a list
@pytest.fixture
def filter_empty_df (read_parquet_files):
  dfs = read_parquet_files
  for df in dfs:
    if df.count() < 1:
      return False
  return True

# This method checks all columns of all dataframes created @ read_parquet_files()
# find_empty_column(read_parquet_files): returns False if finds an empty column (Null)
# otherwise it returns True
# read_parquet_files: Method, should return a list
@pytest.fixture
def find_empty_column (read_parquet_files):
  dfs = read_parquet_files
  for df in dfs:
    cols = df.columns
    for col in cols:
      if (not df[col].isNotNull):
        return False
  return True

# This test checks if the return of the method read_parquet_files() is not None
def test_read_parquet_files(read_parquet_files):
  assert read_parquet_files is not None 
  
# This test checks if the return of the method filter_empty_df() is True
def test_filter_empty_df(filter_empty_df):
  assert filter_empty_df is True

# This test checks if the return of the method find_empty_column() is True
def test_find_empty_column (find_empty_column):
  assert find_empty_column is True

#Execute command: $ pytest test_universo_interaccion.py
