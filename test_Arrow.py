from pyarrow import dataset, Table, parquet
from datetime import datetime


def read_csvs(file_locations: str) -> Table:
    ds = dataset.dataset(file_locations,  format="csv")
    return ds.to_table()


def calculate_metrics(tb: Table) -> Table:
    metrics = tb.group_by("date").aggregate([("failure", "sum")])
    return metrics


def write_parquet_metrics(tb: Table) -> None:
    parquet.write_to_dataset(tb,
                             'data/data_out',
                             partition_cols=['date'],
                             existing_data_behavior='delete_matching'
                             )


if __name__ == '__main__':
    # Using open source data set https://www.backblaze.com/b2/hard-drive-test-data.html
    t1 = datetime.now()
    hard_drive_data = read_csvs('data/data_Q1_2022/')
    metrics = calculate_metrics(hard_drive_data)
    write_parquet_metrics(metrics)
    t2 = datetime.now()
    print("it took {x}".format(x=t2-t1))
