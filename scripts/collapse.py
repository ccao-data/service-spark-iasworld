from pyarrow import dataset as ds

data = ds.dataset("target/test/", format="parquet", partitioning="hive")

file_options = ds.ParquetFileFormat().make_write_options(compression='zstd')

ds.write_dataset(
    data,
    "target/test2/",
    format="parquet",
    partitioning=["taxyr", "cur"],
    partitioning_flavor="hive",
    file_options=file_options,
    max_rows_per_file=5*10**6
)
