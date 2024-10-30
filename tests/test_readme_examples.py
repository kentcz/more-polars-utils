import tempfile
import unittest

import polars as pl

import more_polars_utils.examples.small as more_examples
from more_polars_utils.common.dataframe_assets import PolarsParquetAsset, ACTIVE_PROJECT, ProjectConfiguration


class ReadmeTestCase(unittest.TestCase):

    def setUp(self):
        # Create temporary directories for the project and scratch paths
        self.temporary_project_dir = tempfile.TemporaryDirectory()
        self.temporary_scratch_dir = tempfile.TemporaryDirectory()

        # Set the project configuration
        ACTIVE_PROJECT.set_configuration(
            ProjectConfiguration(
                project_name="test_project",
                asset_path=self.temporary_project_dir.name,
                scratch_path=self.temporary_scratch_dir.name,
            )
        )

    def test_print_count_example(self):
        customer_1_orders = (
            more_examples.orders_df
            .more_print_count("orders_df rows count")

            .filter(pl.col("customer_id") == 1)
            .more_print_count("customer_1_orders rows count")
        )

        self.assertEqual(6, more_examples.orders_df.height)
        self.assertEqual(3, customer_1_orders.height)

    def test_frequency_count_example(self):
        customer_order_frequency = (
            more_examples.orders_df
            .join(more_examples.customers_df, on="customer_id")
            .more_frequency_count("customer_name")
        )

        print(customer_order_frequency)

        self.assertEqual(3, customer_order_frequency.height)

        expected_df = pl.DataFrame([
            {"customer_name": "Alice", "count": 3, "frequency": 3.0 / 6},
            {"customer_name": "Bob", "count": 2, "frequency": 2.0 / 6},
            {"customer_name": "Calvin", "count": 1, "frequency": 1.0 / 6},
        ])
        self.assertTrue(expected_df.frame_equal(customer_order_frequency))

    def test_asset_example(self):
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

        alice_orders_df = alice_orders(customer_orders_df)
        print(alice_orders_df)

        expected_df = pl.DataFrame([
            {"order_id": 1, "customer_name": "Alice", "order_date": "2021-01-01", "total": 100.0},
            {"order_id": 2, "customer_name": "Alice", "order_date": "2021-01-02", "total": 200.0},
            {"order_id": 6, "customer_name": "Alice", "order_date": "2021-01-06", "total": 600.0},
        ])
        self.assertTrue(expected_df.frame_equal(alice_orders_df))


if __name__ == '__main__':
    unittest.main()
