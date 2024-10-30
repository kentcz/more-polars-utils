# more-polars-utils

Utility functions for [polars](https://pola.rs/)

This library expands the [polars.DataFrame](https://docs.pola.rs/api/python/stable/reference/dataframe/index.html) with utility functions for common data engineering operations. This collection of generic utility functions aims to improve the readability and maintainability of Polars-based projects.

## Installation

```bash
pip install more-polars-utils
```

## Usage

### Additional DataFrame methods

Importing the `more_polars_utils` module will extend the `polars.DataFrame` class with a few utility functions. These new methods are prefixed with `more_` to avoid conflicts with existing methods.

```python
import more_polars_utils
```

The `more_print_count()` is an inline method prints the number of rows in the DataFrame. This method is useful tracking the size of the dataframe during transformations.

```python
import polars as pl
import more_polars_utils.examples.small as more_examples

customer_1_orders = (
    more_examples.orders_df
    .more_print_count("orders_df rows count")

    .filter(pl.col("customer_id") == 1)
    .more_print_count("customer_1_orders rows count")
)

# Output:
# orders_df rows count: 5
# customer_1_orders rows count: 2
```

The `more_frequency_count()` method calculates the frequency of each unique value in a column, a common operation for examining a dataframe.
```python
import polars as pl
import more_polars_utils.examples.small as more_examples

customer_order_frequency = (
    more_examples.orders_df
    .join(more_examples.customers_df, on="customer_id")
    .more_frequency_count("customer_name")
)

print(customer_order_frequency)

# Output:
# ┌───────────────┬───────┬───────────┐
# │ customer_name ┆ count ┆ frequency │
# │ ---           ┆ ---   ┆ ---       │
# │ str           ┆ u32   ┆ f64       │
# ╞═══════════════╪═══════╪═══════════╡
# │ Alice         ┆ 3     ┆ 0.5       │
# │ Bob           ┆ 2     ┆ 0.333333  │
# │ Calvin        ┆ 1     ┆ 0.166667  │
# └───────────────┴───────┴───────────┘
```

### DataFrame Assets

The `PolarsParquetAsset` class simplifies the management of DataFrame assets, with dependency tracking and caching of intermediate transformations. 

```python
import polars as pl
import more_polars_utils.examples.small as more_examples
from more_polars_utils.common.dataframe_assets import PolarsParquetAsset

@PolarsParquetAsset.decorator(asset_name="customer_orders")
def customer_orders(orders: pl.DataFrame, customers: pl.DataFrame) -> pl.DataFrame:
    return (
        orders
        .join(customers, on="customer_id")
        .select(["order_id", "customer_name", "order_date", "total"])
        .sort("order_id")
    )

@PolarsParquetAsset.decorator(asset_name="alice_orders")
def alice_orders(customer_orders: pl.DataFrame) -> pl.DataFrame:
    return (
        customer_orders
        .filter(pl.col("customer_name") == "Alice")
        .sort("order_date")
    )

# Build the asset
customer_orders_df = customer_orders(more_examples.orders_df, more_examples.customers_df)
print(customer_orders_df)

# Output:
# ┌──────────┬───────────────┬────────────┬───────┐
# │ order_id ┆ customer_name ┆ order_date ┆ total │
# │ ---      ┆ ---           ┆ ---        ┆ ---   │
# │ i64      ┆ str           ┆ str        ┆ f64   │
# ╞══════════╪═══════════════╪════════════╪═══════╡
# │ 1        ┆ Alice         ┆ 2021-01-01 ┆ 100.0 │
# │ 2        ┆ Alice         ┆ 2021-01-02 ┆ 200.0 │
# │ 3        ┆ Bob           ┆ 2021-01-03 ┆ 300.0 │
# │ 4        ┆ Bob           ┆ 2021-01-04 ┆ 400.0 │
# │ 5        ┆ Calvin        ┆ 2021-01-05 ┆ 500.0 │
# │ 6        ┆ Alice         ┆ 2021-01-06 ┆ 600.0 │
# └──────────┴───────────────┴────────────┴───────┘

alice_orders_df = alice_orders(customer_orders_df)
print(alice_orders_df)

# Output:
# ┌──────────┬───────────────┬────────────┬───────┐
# │ order_id ┆ customer_name ┆ order_date ┆ total │
# │ ---      ┆ ---           ┆ ---        ┆ ---   │
# │ i64      ┆ str           ┆ str        ┆ f64   │
# ╞══════════╪═══════════════╪════════════╪═══════╡
# │ 1        ┆ Alice         ┆ 2021-01-01 ┆ 100.0 │
# │ 2        ┆ Alice         ┆ 2021-01-02 ┆ 200.0 │
# │ 6        ┆ Alice         ┆ 2021-01-06 ┆ 600.0 │
# └──────────┴───────────────┴────────────┴───────┘
```

