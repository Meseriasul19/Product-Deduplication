import pandas as pd
import numpy as np
import hashlib

# Reading the dataset
df = pd.read_parquet("veridion_product_deduplication_challenge.snappy.parquet")  

# Convert empty arrays and dictionaries to NaN
def set_nan_values(data):
    if isinstance(data, np.ndarray):  
        if len(data) == 0:
            return np.nan  
    elif isinstance(data, dict): 
        if len(data) == 0 or all(value is None or not value for value in data.values()):
            return np.nan  
    return data

consolidated_df = df.apply(lambda col: col.apply(set_nan_values))

# Normalizing data maintaining accurate and consistent records for hashing
def normalize_text(text):
    if pd.isna(text):
        return ""
    return str(text).lower().strip()

# Generate a unique key for each product
def create_product_key(row):
    unspsc = normalize_text(row.get("unspsc", ""))
    name = normalize_text(row.get("product_name", ""))
    base = unspsc + "|" + name
    return hashlib.md5(base.encode()).hexdigest()

consolidated_df["product_key"] = consolidated_df.apply(create_product_key, axis=1)

# Merge duplicated products into a single entry per product
# Ensuring uniqueness with multiple details of a product organised into a single entry
def merge_group(group):
    merged = {}
    for col in group.columns:
        if col == 'product_key':
            merged[col] = group[col].iloc[0]
        else:
            vals = group[col].dropna().drop_duplicates()
            if len(vals) == 0:
                merged[col] = np.nan
            elif group[col].dtype == object:
                merged[col] = " | ".join(map(str, vals))
            else:
                merged[col] = vals.iloc[0] if not vals.empty else np.nan
    return pd.Series(merged)

# Group and merge duplicates
consolidated_df = consolidated_df.groupby("product_key").apply(merge_group).reset_index(drop=True)

# Dropping product_key column
consolidated_df.drop(columns=["product_key"], inplace=True)

# Save the new consolidated dataset
consolidated_df.to_parquet("consolidated_df.parquet", index=False)