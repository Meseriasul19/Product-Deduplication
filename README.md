## Product Deduplication Project

### Project Overview

This project processes and deduplicates product data extracted from various web sources using LLMs (Large Language Models). 
The dataset contains product details extracted from various web pages using LLMs, resulting in duplicate entries where the same product appears across different sources. Each row represents partial attributes of a product.
The goal is to merge similar products while ensuring uniqueness and maintaining data integrity.

### Dataset Description

The dataset is stored in a Parquet file (`veridion_product_deduplication_challenge.snappy.parquet`) and contains multiple attributes describing the products, such as:

- **unspsc**: A standardized classification code for products
- **product\_title**: The title of the product
- **product\_name**: The name of the product 
- **brand**: The brand associated with the product
- **Other attributes** providing additional product details

## Steps to Process the Data

### 1. Data Analysis (`info.py`)

Before deduplication, we analyze the dataset to understand its structure and identify potential inconsistencies. The `info.py` script performs:

- **Data Type Analysis**: Identifies types of data within each column, including empty arrays and dictionaries.
- **Missing Value Inspection**: Checks if missing values contain alternative relevant information.
- **Duplicate Count Estimation**: Analyzes which columns are most effective for identifying duplicate products.

### 2. Data Deduplication (`main.py`)

Once the data is analyzed, the `main.py` script performs the deduplication process:

#### **Preprocessing:**

- Converts empty arrays and dictionaries to NaN for consistency.
- Normalizes text data to ensure uniformity.

#### **Generating Unique Product Keys:**

- A unique `product_key` is created using the `unspsc` and `product_name` attributes.
- A hash function (MD5) is applied to create a unique identifier for each product.

#### **Merging Duplicates:**

- Products sharing the same `product_key` are grouped together.
- Unique values from duplicated entries are combined into a single record.
- Text-based attributes are merged using a separator (`|`).

#### **Saving the Cleaned Dataset:**

- The final deduplicated dataset is saved as `consolidated_df.parquet`.

## How to Use This Project

1. **Analyze the Dataset:**
   ```bash
   python info.py
   ```
2. **Deduplicate the Products:**
   ```bash
   python main.py
   ```
3. **Compare the datasets:**
   ```bash
   python compare.py
   ```

## Future Improvements

- Implementing more advanced similarity measures for deduplication.
- Using machine learning to identify duplicates with higher accuracy.
- Enhancing performance for large-scale datasets.

## Conclusion

This project provides a structured approach to deduplicating product data, ensuring unique and high-quality records.


