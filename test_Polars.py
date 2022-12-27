import polars as pl
from pyarrow import parquet
from datetime import datetime

d1 = datetime.now()
q = (
    pl.scan_csv("data/data_Q1_2022/*.csv", parse_dates=True, dtypes={
                            'date': pl.Date,
                            'serial_number' : pl.Utf8,
                            'model' : pl.Utf8,
                            'capacity_bytes' : pl.Utf8,
                            'failure' : pl.Int32
                            })
)
df = q.lazy().groupby(pl.col("date")).agg(pl.col('failure').sum()).collect()
parquet.write_to_dataset(df.to_arrow(),
                             'data/data_out',
                             partition_cols=['date'],
                             existing_data_behavior='delete_matching'
                             )
d2 = datetime.now()
print(d2-d1)
