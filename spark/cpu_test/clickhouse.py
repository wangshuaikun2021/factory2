from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

# 创建SparkSession
spark = SparkSession.builder \
    .appName("Read ClickHouse Data") \
    .config("spark.executor.cores", 12) \
    .config("spark.executor.memory", "50g") \
    .config("spark.master", "spark://VM-0-32-ubuntu:7077") \
    .config("spark.driver.host", "192.168.3.238") \
    .config("spark.driver.bindAddress", "192.168.3.238") \
    .getOrCreate()
# spark = SparkSession.builder \
#     .appName("Read ClickHouse Data") \
#     .getOrCreate()

# 读取 ClickHouse 数据
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:clickhouse://10.9.0.32:8123/spark_test") \
    .option("dbtable", "(SELECT id, arrayStringConcat(feature, ',') as feature_str FROM face_features) tmp") \
    .option("user", "default") \
    .load()
df = df.withColumn("feature", split(col("feature_str"), ",").cast("array<float>"))
# 打印记录数
print(f"Total records: {df.count()}")
print(df.head())

# 停止SparkSession
spark.stop()
