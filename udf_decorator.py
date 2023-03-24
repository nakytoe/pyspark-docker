import pandas as pd
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

# decorate with pandas_udf to transfer data efficiently between
# spark and python, and allow vectorized calculation via pandas
@pandas_udf(returnType = LongType())
def cubed(s:pd.Series)->pd.Series:
    return s*s*s

if __name__ == "__main__":
    # run with:
    # docker run -it --entrypoint "/opt/spark/bin/spark-submit" pyspark udf_decorator.py 
    # where pyspark is the name of the container

    # create SparkSession
    from pyspark.sql import SparkSession
    spark = (SparkSession
             .builder
             .master("local")
             .appName("udf-test")
             .getOrCreate())

    # register udf
    spark.udf.register("cubed", cubed)

    # use with DataFrame
    df = spark.range(1, 9)
    df.select("id", cubed(col("id"))).show()

    # use in SQL
    df.createOrReplaceTempView("udf_test")
    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()