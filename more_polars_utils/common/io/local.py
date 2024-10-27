import os
from datetime import datetime, timezone
from glob import glob
from os import PathLike
from typing import Union, Optional

import polars as pl


def file_exists(path: Union[str, PathLike[str]]) -> bool:
    return os.path.exists(path)


def is_directory(path: Union[str, PathLike[str]]) -> bool:
    return os.path.isdir(path)


def make_directories(path: Union[str, PathLike[str]], *args, **kwargs):
    os.makedirs(path, *args, **kwargs)


def file_last_modified(path: Union[str, PathLike[str]]) -> datetime:
    assert (file_exists(path))
    file_timestamp = os.path.getmtime(path)
    return datetime.utcfromtimestamp(file_timestamp).replace(tzinfo=timezone.utc)


def list_nested_partitions(path: Union[str, PathLike[str]], file_extension="parquet") -> list[str]:
    formatted_path = str(path)[:-1] if str(path).endswith('/') else str(path)
    relevant_files = glob(f'{formatted_path}/**/*.{file_extension}', recursive=True)
    return relevant_files


def read_parquet(path: str, *args, **kwargs) -> pl.DataFrame:
    assert (file_exists(path))
    if is_directory(path):
        formatted_path = str(path)[:-1] if str(path).endswith('/') else str(path)
        return pl.read_parquet(f"{formatted_path}/**/*.parquet", *args, **kwargs)
    else:
        return pl.read_parquet(path, *args, **kwargs)


def write_parquet(df: pl.DataFrame, path: str, *args, **kwargs):
    df.write_parquet(path, *args, **kwargs)


def parquet_file_size(path: str, file_extension: str = "parquet", **kwargs) -> Optional[int]:
    assert (file_exists(path))

    if is_directory(path):
        partitions = list_nested_partitions(path=path, file_extension=file_extension)
    else:
        partitions = [path]

    partition_sizes = [
        os.path.getsize(partition)
        for partition in partitions
    ]

    return sum(partition_sizes)
