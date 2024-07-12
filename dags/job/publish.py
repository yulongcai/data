from pyspark.sql import SparkSession


def publish(input_file, output_file):
    spark = SparkSession.builder \
        .appName("load") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.warehouse.dir", "/Users/yulong.cai/data_develop/airflow/table") \
        .getOrCreate()
    df = spark.read.csv(input_file, header=True, inferSchema=True)
    df.toPandas().to_excel(output_file, index=False)
