from pyspark.sql import SparkSession
import time

spark = SparkSession \
    .builder \
    .appName('standalone') \
    .getOrCreate()

schema = 'id INT, country STRING, hit LONG'
df = spark.createDataFrame(data=[(1,'korea', 120),(2,'USA',80),(3,'Japan',40)], schema=schema)
print(df.count())
# sleep 10 minute
time.sleep(600)

# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, IntegerType, LongType, StringType, StructField
# import time
#
# spark = SparkSession \
#         .builder \
#         .appName('standalone') \
#         .getOrcreate()
#
# schema = StructType([
#     StructField('id', IntegerType(), True),
#     StructField('country', StringType(), True),
#     StructField('hit', LongType(), True),
# ])
#
# data = [(1,'Korea',120),(2,'USA',80),(3,'Japan',40)]
# df = spark.createDataFrame(data=data, schema=schema)
#
# df.show()
# time.sleep(500)