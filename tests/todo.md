
## Add polars dataframe
something like this:
```
import polars as pl

# Iterate in chunks of 10,000 rows (default)
for chunk_df in df.iter_slices(n_rows=5000):
    # Process each chunk as a Polars DataFrame
    print(chunk_df.shape)
```