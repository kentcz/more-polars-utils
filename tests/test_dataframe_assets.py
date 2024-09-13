import os
import tempfile
import unittest
import polars as pl

from polars.testing import assert_frame_equal
from more_polars_utils.common.dataframe_assets import PolarsParquetAsset, ACTIVE_PROJECT, ProjectConfiguration


class PolarsParquetAssetTestCase(unittest.TestCase):

    def setUp(self):
        self.sample_df = pl.DataFrame(
            {
                "order_id": ["a", "b", "c"],
                "customer_id": [1, 2, 2],
                "amount": [100, 200, 300],
            }
        )

        # Create temporary directories for the project and scratch paths
        self.temporary_project_dir = tempfile.TemporaryDirectory()
        self.temporary_scratch_dir = tempfile.TemporaryDirectory()

        # Setup the project configuration
        ACTIVE_PROJECT.set_configuration(
            ProjectConfiguration(
                project_name="test_project",
                asset_path=self.temporary_project_dir.name,
                scratch_path=self.temporary_scratch_dir.name,
            )
        )

    def tearDown(self):
        self.temporary_project_dir.cleanup()
        self.temporary_scratch_dir.cleanup()

    def test_simple_asset(self):
        # Define a new dataframe asset
        @PolarsParquetAsset.decorator()
        def new_dataframe() -> pl.DataFrame:
            return self.sample_df

        # Build the asset
        new_df = new_dataframe()

        # Check that the asset was correctly built
        assert_frame_equal(new_df, self.sample_df)

        # Check that the asset was saved to the correct location
        expected_path = f"{self.temporary_project_dir.name}/new_dataframe.parquet"
        self.assertTrue(os.path.exists(expected_path))

    def test_named_asset(self):
        asset_name = "my_test_asset_123"

        # Define a new dataframe asset
        @PolarsParquetAsset.decorator(asset_name=asset_name)
        def new_dataframe() -> pl.DataFrame:
            return self.sample_df

        # Build the asset
        new_df = new_dataframe()

        # Check that the asset was correctly built
        assert_frame_equal(new_df, self.sample_df)

        # Check that the asset was saved to the correct location
        expected_path = f"{self.temporary_project_dir.name}/{asset_name}.parquet"
        self.assertTrue(os.path.exists(expected_path))

    def test_temporary_asset(self):
        # Define a new dataframe asset
        @PolarsParquetAsset.decorator(is_temporary=True)
        def new_dataframe() -> pl.DataFrame:
            return self.sample_df

        # Build the asset
        new_df = new_dataframe()

        # Check that the asset was correctly built
        assert_frame_equal(new_df, self.sample_df)

        # Check that the asset was saved to the correct location
        expected_path = f"{self.temporary_scratch_dir.name}/new_dataframe.parquet"
        self.assertTrue(os.path.exists(expected_path))

    def test_cached_asset(self):
        # Define a new dataframe asset
        @PolarsParquetAsset.decorator(is_temporary=True)
        def new_dataframe() -> pl.DataFrame:
            return pl.DataFrame()

        # Create a cached asset, so that is does not need to be recomputed
        expected_path = f"{self.temporary_scratch_dir.name}/new_dataframe.parquet"
        self.sample_df.write_parquet(expected_path)

        # Build the asset, which should be read from the cache
        new_df = new_dataframe()

        # Check that the returned dataframe matches the cached dataframe
        assert_frame_equal(new_df, self.sample_df)

        # Check that the asset was saved to the correct location
        self.assertTrue(os.path.exists(expected_path))
