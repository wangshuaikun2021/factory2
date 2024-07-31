from dask.distributed import Client, SSHCluster
import dask

if __name__ == "__main__":
    cluster = SSHCluster(
        hosts=["192.168.3.238", "192.168.3.238"],
        connect_options={"known_hosts": None}
    )
    client = Client(cluster)

    df = dask.datasets.timeseries()
    df2 = df[df.y > 0].groupby("name").x.std()
    res = df2.compute()
    print(res)

