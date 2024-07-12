import os
from functools import reduce

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("load") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.warehouse.dir", "/Users/yulong.cai/data_develop/airflow/table") \
    .getOrCreate()


def getAllFiles():
    root_dir = "/Users/yulong.cai/data_develop/airflow/data"
    folder_paths = []

    for item in os.listdir(root_dir):
        item_path = os.path.join(root_dir, item)
        if os.path.isdir(item_path):
            folder_paths.append(item_path)
    return folder_paths


def process(sql, output):
    files = getAllFiles()

    dfs = [spark.read.csv(f, header=True, inferSchema=True) for f in files]
    df_new = [f.select("date", "serial_number", "model", "capacity_bytes", "failure") for f in dfs]
    df = reduce(lambda df1, df2: df1.union(df2), df_new)
    df.createOrReplaceTempView("data")
    result = spark.sql(sql)
    result.write.csv(output, mode="overwrite", header=True)
