import polars as pl
from typing import Optional, Sequence

from polars import Expr
from more_polars_utils.common.io import write_parquet, write_csv
from polars.type_aliases import IntoExpr


def optional_limit(self: pl.DataFrame, limit: Optional[int] = None) -> pl.DataFrame:
    df = self
    if limit:
        df = self.head(limit)
    return df


def print_count(self: pl.DataFrame, label: Optional[str] = None) -> pl.DataFrame:
    """
        Print the datafame count without terminating a method chain

        :param self: The dataframe
        :param label: Optional label to prefix the print statement
        :return: The dataframe
    """

    if label:
        print(f"{label}: {len(self):,}")
    else:
        print(f"{len(self):,}")

    return self


def frequency_count(
        self: pl.DataFrame,
        group_by_column: IntoExpr,
        count_column: str = "count",
        frequency_column: str = "frequency"
) -> pl.DataFrame:
    """
    Group by `group_by_column` then generate `count` and `frequency` columns for the grouped data

    :param self: The dataframe
    :param group_by_column: The column to group by
    :param count_column: The desired name for the `count` column
    :param frequency_column: The desired name for the `frequency` column
    :return: The dataframe
    """
    df_count = len(self)

    return (
        self
        .group_by(group_by_column)
        .agg(
            pl.len().alias(count_column)
        )
        .with_columns(
            **{frequency_column: (pl.col(count_column) / pl.lit(df_count))}
        )
        .sort(count_column, descending=True)
    )


def check_unique(self: pl.DataFrame, subset: str | Expr | Sequence[str | Expr] | None = None) -> bool:
    """
    Check if a column has unique values

    :param self: The dataframe
    :param subset: One or more columns in the dataframe
    :return: True if the column has unique values, False otherwise
    """

    return self.height == self.n_unique(subset)


def print_csv(self: pl.DataFrame, limit: Optional[int] = None) -> None:
    """
    Print the first `limit` rows of the dataframe in CSV format

    :param self: The dataframe
    :param limit: The number of rows to print
    """

    print(
        optional_limit(self, limit).write_csv()
    )


def show(self: pl.DataFrame, limit: int = 20) -> None:
    """
    Print the first `limit` rows of the dataframe

    :param self: The dataframe
    :param limit: The number of rows to print, if limit <= 0, print all rows
    """

    with pl.Config(tbl_rows=limit):
        print(self)


def show_vertical(self: pl.DataFrame, limit: int = 1) -> None:
    """
    Print the first `limit` rows of the dataframe in CSV format

    :param self: The dataframe
    :param limit: The number of rows to print, if limit <= 0, print all rows
    """

    vertical_df: pl.DataFrame = optional_limit(self, limit).transpose(include_header=True)

    with pl.Config(tbl_rows=vertical_df.height):
        print(vertical_df)


# Add the methods to the DataFrame class
pl.DataFrame.more_print_count = print_count            # type: ignore[attr-defined]
pl.DataFrame.more_frequency_count = frequency_count    # type: ignore[attr-defined]
pl.DataFrame.more_check_unique = check_unique          # type: ignore[attr-defined]
pl.DataFrame.more_print_csv = print_csv                # type: ignore[attr-defined]
pl.DataFrame.more_show = show                          # type: ignore[attr-defined]
pl.DataFrame.more_show_vertical = show_vertical        # type: ignore[attr-defined]
pl.DataFrame.more_write_parquet = write_parquet        # type: ignore[attr-defined]
pl.DataFrame.more_write_csv = write_csv                # type: ignore[attr-defined]
