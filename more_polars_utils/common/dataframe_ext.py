import polars as pl

def print_count(self: pl.DataFrame) -> pl.DataFrame:
	print(f"{len(self):,}")
	return self
