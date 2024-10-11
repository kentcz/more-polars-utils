from datetime import datetime
from typing import Optional

import polars as pl
import s3fs  # type: ignore

S3_FILESYSTEM = s3fs.S3FileSystem()


def is_s3_path(path: str) -> bool:
    return path.startswith("s3://") or path.startswith("s3a://")


def file_exists(path: str) -> bool:
    return S3_FILESYSTEM.exists(path)


def is_directory(path: str) -> bool:
    return S3_FILESYSTEM.isdir(path)


def make_directories(path: str, *args, **kwargs):
    S3_FILESYSTEM.makedirs(path, *args, **kwargs)


def file_last_modified(path: str) -> datetime:
    assert (file_exists(path))
    file_info = S3_FILESYSTEM.info(path)
    return file_info["LastModified"]


def list_nested_partitions(path: str, file_extension="parquet", s3_protocol="s3://") -> list[str]:
    """
    Lists all `.parquet` files in a given S3 directory, including those in nested directories.

    Parameters:
    path (str): The S3 directory to search (e.g., 's3://bucket-name/path/to/directory')

    Returns:
    list: A list of `.parquet` file paths.
    """

    # Ensure the S3 directory ends with a slash (for directory context)
    dir_path = path if path.endswith('/') else path + '/'

    # List all files and directories in the given S3 directory and its subdirectories
    all_files = S3_FILESYSTEM.glob(dir_path + '**')  # '**' allows for recursive search in subdirectories

    # Filter files based on the file extension
    relevant_files = [
        f"{s3_protocol}{f}"
        for f in all_files
        if f.endswith(file_extension) and f"{s3_protocol}{f}" != path
    ]

    return relevant_files


def write_parquet(df: pl.DataFrame, path: str, *args, **kwargs):
    with S3_FILESYSTEM.open(path, "wb") as f:
        df.write_parquet(f, *args, **kwargs)


def write_csv(df: pl.DataFrame, path: str, *args, **kwargs):
    with S3_FILESYSTEM.open(path, "wb") as f:
        df.write_csv(f, *args, **kwargs)


def read_parquet(path: str, *args, **kwargs) -> pl.DataFrame:
    assert (file_exists(path))
    if is_directory(path):
        formatted_path = str(path)[:-1] if str(path).endswith('/') else str(path)
        return pl.read_parquet(f"{formatted_path}/**/*.parquet", *args, **kwargs)
    else:
        return pl.read_parquet(path, *args, **kwargs)


def parquet_file_size(path: str, file_extension: str = "parquet", **kwargs) -> Optional[int]:
    assert (file_exists(path))

    if is_directory(path):
        partitions = list_nested_partitions(path=path, file_extension=file_extension, **kwargs)
    else:
        partitions = [path]

    partition_sizes = [
        S3_FILESYSTEM.info(partition).get("size", 0)
        for partition in partitions
    ]

    return sum(partition_sizes)
