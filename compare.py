import pandas as pd

# Reading the datasets

consolidated_df = pd.read_parquet("consolidated_df.parquet")
df = pd.read_parquet("veridion_product_deduplication_challenge.snappy.parquet")

# Print shapes
print("The initial datatset shape: ", df.shape)
print("The new datatset shape:  ", consolidated_df.shape)
# 5131 products dropped