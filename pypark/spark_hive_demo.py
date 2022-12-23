import findspark
findspark.init()

import pyspark

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

warehouse_location=r'E:\temp\hive\spark-warehouse'
spark = SparkSession.builder \
        .master("local") \
        .appName("PySpark on hive demo") \
        .config("spark.sql.warehouse.dir",warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()
sc = spark.sparkContext

df = spark.createDataFrame([
        (1, 144.5, 5.9, 33, 'M'),
        (2, 167.2, 5.4, 45, 'M'),
        (3, 124.1, 5.2, 23, 'F'),
        (4, 144.5, 5.9, 33, 'M'),
        (5, 133.2, 5.7, 54, 'F'),
        (3, 124.1, 5.2, 23, 'F'),
        (5, 129.2, 5.3, 42, 'M'),
    ], ['id', 'weight', 'height', 'age', 'gender'])

df.createOrReplaceTempView("result")
# spark.sql("create table people1 as select * from result")
spark.sql("select id,age from people1").show()

