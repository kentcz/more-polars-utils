import more_polars_utils.common.io.local as io_local  # ignore: type
import more_polars_utils.common.io.s3 as io_s3  # ignore: type


def select_io(path: str):
    if io_s3.is_s3_path(path):
        return io_s3
    else:
        return io_local


def file_exists(path: str) -> bool:
    return select_io(path).file_exists(path)


def is_directory(path: str) -> bool:
    return select_io(path).is_directory(path)


def list_nested_partitions(path: str, file_extension=".parquet", *args, **kwargs) -> list[str]:
    return select_io(path).list_nested_partitions(path, file_extension, *args, **kwargs)


def read_parquet(path: str, *args, **kwargs):
    return select_io(path).read_parquet(path, *args, **kwargs)


def write_parquet(df, path: str, *args, **kwargs):
    return select_io(path).write_parquet(df, path, *args, **kwargs)


def parquet_file_size(path: str) -> int:
    return select_io(path).parquet_file_size(path)
