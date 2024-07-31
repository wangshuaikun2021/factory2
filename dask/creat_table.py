import numpy as np
from clickhouse_driver import Client

# 生成随机的1000x20的数组
array = np.random.rand(1000, 20)

# 连接到ClickHouse数据库
client = Client(host="10.9.0.32", port=9000, database="spark_test")


# 创建表
client.execute('CREATE TABLE IF NOT EXISTS random_array (id Float64, col1 Float64, col2 Float64, col3 Float64, col4 Float64, col5 Float64, col6 Float64, col7 Float64, col8 Float64, col9 Float64, col10 Float64, col11 Float64, col12 Float64, col13 Float64, col14 Float64, col15 Float64, col16 Float64, col17 Float64, col18 Float64, col19 Float64, col20 Float64) ENGINE = MergeTree ORDER BY col1')

# 插入数据
# 添加id
array = np.insert(array, 0, np.arange(1000), axis=1)
client.execute('INSERT INTO random_array VALUES', list(map(tuple, array.tolist())))