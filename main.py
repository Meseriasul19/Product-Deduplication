import pandas as pd
import numpy as np
import hashlib

# Reading the dataset
df = pd.read_parquet("veridion_product_deduplication_challenge.snappy.parquet")  

# Normalizing data maintaining accurate and consistent records for hashing
def normalize_text(text):
    if pd.isna(text):
        return ""
    return str(text).lower().strip()

# Hashing for data integrity and efficient search
def create_product_key(row):
    name = normalize_text(row.get("product_name", ""))
    brand = normalize_text(row.get("brand", ""))
    base = name + "|" + brand
    return hashlib.md5(base.encode()).hexdigest()

df["product_key"] = df.apply(create_product_key, axis=1)

# Merge duplicated products into a single entry per product
# Ensuring uniqueness with multiple details of a product organised into a single entry
def merge_group(group):
    merged = {}
    for col in group.columns:
        if col == 'product_key':
            merged[col] = group[col].iloc[0]
        else:
            vals = group[col].dropna().unique()
            if len(vals) == 0:
                merged[col] = np.nan
            elif group[col].dtype == object:
                merged[col] = " | ".join(map(str, vals))
            else:
                merged[col] = vals[0]
    return pd.Series(merged)

# Grouping by product_key
consolidated_df = df.groupby("product_key").apply(merge_group).reset_index(drop=True)

# Dropping product_key column
consolidated_df.drop(columns=["product_key"], inplace=True)

# Save the new consolidated dataset
consolidated_df.to_parquet("veridion_product_deduplication_challenge.parquet", index=False)