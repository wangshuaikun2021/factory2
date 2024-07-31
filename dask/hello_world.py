import dask
import dask.datasets
import dask.datasets
from dask.distributed import Client

if __name__ == "__main__":
    client = Client("192.168.3.238:8786")
    df = dask.datasets.timeseries()
    df2 = df[df.y > 0].groupby("name").x.std()
    res = df2.compute()
    print(res)