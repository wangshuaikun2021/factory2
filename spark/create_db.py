# -*- encoding: utf-8 -*-
'''
@File    :   create_db.py
@Time    :   2024/07/15 11:25:02
@Author  :   ShuaikunWang
@Contact :   wshuaikun@gmail.com
@Desc    :   None
'''
import os
import sys
sys.path.append(os.getcwd())    # Add the current project path to the PATH
os.chdir(sys.path[0])        # Change the current directory to the path
import numpy as np
# import clickhouse_connect
import clickhouse_driver


if __name__ == "__main__":
    # Step 1: 读取npy文件
    npy_file_path = '/gemini/data-1/cluster_data/data/webface_424_4_feats.npy'
    data = np.load(npy_file_path)

    # 检查数据维度
    n_samples, n_features = data.shape
    print(f"Data shape: {n_samples} samples, {n_features} features")

    # Step 2: 连接ClickHouse数据库
    client = clickhouse_driver.Client(host="10.9.0.32", port=9000, database="spark_test")

    # Step 3: 创建表（如果不存在）
    table_creation_query = """
    CREATE TABLE IF NOT EXISTS face_features (
        id UInt32,
        feature String
    ) ENGINE = MergeTree()
    ORDER BY id
    """
    # client.command(table_creation_query)
    client.execute(table_creation_query)

    # Step 4: 插入数据
    insert_query = "INSERT INTO face_features (id, feature) VALUES"
    insert_data = [(i, ','.join(map(str, data[i].tolist()))) for i in range(n_samples)]
    client.execute(insert_query, insert_data)

    print("Data insertion completed.")