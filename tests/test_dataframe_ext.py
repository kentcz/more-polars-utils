import unittest

import polars as pl

import more_polars_utils.examples.small as more_examples
from more_polars_utils.common.dataframe_ext import print_count, check_unique


class DataFrameTestCase(unittest.TestCase):
    def test_check_unique(self):
        orders_df = more_examples.orders_df

        self.assertTrue(check_unique(orders_df, "order_id"), "order_id should be unique")
        self.assertFalse(check_unique(orders_df, "customer_id"), "customer_id should not be unique")

    def test_frequency_count_example(self):
        customer_order_frequency = (
            more_examples.orders_df
            .join(more_examples.customers_df, on="customer_id")
            .more_frequency_count("customer_name")
        )

        expected_df = pl.DataFrame([
            {"customer_name": "Alice", "count": 3, "frequency": 3.0 / 6},
            {"customer_name": "Bob", "count": 2, "frequency": 2.0 / 6},
            {"customer_name": "Calvin", "count": 1, "frequency": 1.0 / 6},
        ])
        self.assertTrue(expected_df.frame_equal(customer_order_frequency))

    def test_print_count(self):
        orders_df = more_examples.orders_df
        orders_df = print_count(orders_df, "orders_df rows count")
        self.assertEqual(6, len(orders_df))

    def test_show(self):
        orders_df = more_examples.orders_df
        output = orders_df.more_show(limit=4)

        self.assertIsNone(output)

    def test_show_vertical(self):
        orders_df = more_examples.orders_df
        output = orders_df.more_show_vertical()

        self.assertIsNone(output)

    def test_print_csv(self):
        orders_df = more_examples.orders_df
        output = orders_df.more_print_csv()

        self.assertIsNone(output)


if __name__ == '__main__':
    unittest.main()
