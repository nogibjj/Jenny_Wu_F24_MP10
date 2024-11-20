"""
Library functions for ETL and queries
"""

import os
import requests
import pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

LOG_FILE = "pyspark_output.md"

def log_output(operation, output, query=None):
    """Generates a markdown file for output"""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if query:
            file.write(f"The query is {query}\n\n")
        file.write("The truncated output is: \n\n")
        file.write(output)
        file.write("\n\n")


def start_spark(appName):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark


def end_spark(spark):
    spark.stop()
    return "stopped spark session"


def extract(
    url="https://raw.githubusercontent.com/nogibjj/Jenny_Wu_F24_IP2/refs/heads/main/rust_files/data/nypd_shooting.csv",
    file_path="data/nypd_shooting.csv",
    directory="data",
):
    """Extracts a url to a file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)

    return file_path


def load_data(
    spark,
    data="data/nypd_shooting.csv",
    name="nypd_shooting",
):
    """Loads data into file"""
    # data preprocessing by setting schema
    schema = StructType(
        [
            StructField("incident_key", IntegerType(), True),
            StructField("occur_date", StringType(), True),
            StructField("boro", StringType(), True),
            StructField("vic_sex", StringType(), True),
            StructField("vic_race", StringType(), True),
        ]
    )

    df = spark.read.option("header", "true").schema(schema).csv(data)

    log_output("Loading data", df.limit(10).toPandas().to_markdown())

    return df


def query(spark, df, query, name):
    """Queries using spark, puts into md file"""
    df = df.createOrReplaceTempView(name)

    log_output("query data", spark.sql(query).toPandas().to_markdown(), query)

    return spark.sql(query).show()


def transform(df):
    """Transformation on the data"""
    conditions = [
        (col("boro") == "BRONX"),
        (col("boro") == "QUEENS"),
        (col("boro") == "BROOKLYN"),
        (col("boro") == "MANHATTAN") | (col("boro") == "STATEN ISLAND"),
    ]

    categories = ["North", "East", "West", "South"]

    df = df.withColumn(
        "Boro Categories",
        when(conditions[0], categories[0])
        .when(conditions[1], categories[1])
        .when(conditions[2], categories[2])
        .when(conditions[3], categories[3])
        .otherwise("Other"),
    )

    log_output("Transforming data", df.limit(10).toPandas().to_markdown())

    return df.show()