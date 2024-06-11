import polars as pl
from typing import Optional


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
        group_by_column,
        count_column="count",
        frequency_column="frequency"
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
            pl.count("*").alias(count_column)
        )
        .with_columns((pl.col(count_column) / pl.lit(df_count)).alias(frequency_column))
        .sort(count_column, descending=True)
    )


# Add the methods to the DataFrame class
pl.DataFrame.print_count = print_count          # type: ignore[attr-defined]
pl.DataFrame.frequency_count = frequency_count  # type: ignore[attr-defined]
