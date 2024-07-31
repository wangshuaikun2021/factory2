import dask.dataframe as dd
from dask.distributed import LocalCluster, Client
import pandas as pd
from clickhouse_driver import Client as Client_click

if __name__ == "__main__":
    cluster = LocalCluster()
    client = Client(cluster)
    # # engine = 'clickhouse://default:@10.9.0.32:8123/spark_test'
    # engine = create_engine('clickhouse+http://default:@10.9.0.32:8123/spark_test')
    # ddf = dd.read_sql_table("random_array", engine, index_col='id', npartitions=10)

    # dd = dask.datasets.timeseries(start='2000-01-01', end='2000-01-02', dtypes={'x': float, 'y': float})
    # res = dd.compute()
    # print(res)
    # result = ddf.head()

    # print(result)
    # client.close()

    def read_clickhouse_to_dask(host, database, table, user='default', password=''):
        # 创建 ClickHouse 连接
        client = Client_click(host=host, user=user, password=password, database=database)
        
        # 查询数据
        query = f'SELECT * FROM {table}'
        data = client.execute(query, with_column_types=True)
        print(data)
        # 获取列名和数据类型
        columns = [col[0] for col in data[1]]
        df = pd.DataFrame(data[0], columns=columns)
        
        # 将数据转换为 Dask DataFrame
        ddf = dd.from_pandas(df, npartitions=3)
        
        return ddf

    # 使用示例
    host = '10.9.0.32'
    database = 'spark_test'
    table = 'random_array'
    index_col = 'id'  # 需要确保表中有一个唯一的索引列

    ddf = read_clickhouse_to_dask(host, database, table, index_col)
    print(ddf.head())
