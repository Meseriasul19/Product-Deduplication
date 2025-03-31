import pandas as pd
import numpy as np
from collections import defaultdict

# Reading the dataset
df = pd.read_parquet("veridion_product_deduplication_challenge.snappy.parquet")

# Display dataset dimensions
print(df.shape, "\n")
# Observation: The dataset is relatively small in size

# Display data types of each column
print(df.dtypes, "\n")
# Observation: Most attributes are stored as 'object' types except for the manufacturing year (int32)

def print_detailed_datatypes(column):

    """
    Function to analyze the detailed data types within a column.
    It identifies occurrences of different data types, including empty arrays and dictionaries.
    """
     
    datatypes = defaultdict(int)
    for data in column:

        # Count occurrences of each type
        datatypes[type(data)] += 1

        # Handle NumPy arrays
        if(type(data) == np.ndarray):

            # if empty array => TO DO: set to np.nan
            if(len(data) == 0):
                datatypes["Empty arrays"] += 1

            else:

                # set for storing datatypes
                types = set()

                for element in data:
                    types.add(type(element))

                    # arrays can have dict data types
                    if(type(element) == dict):

                        if(len(element) == 0 or all(value is None or not value for value in element.values())):
                            datatypes["Empty dict"] += 1
                        else:
                            for index, value in element.items():
                                if(value is not None):
                                    datatypes[index] = type(value)

                # counting array elements and storing their types
                for element_type in types:
                    datatypes[element_type] += 1
        
        # Handle dictionaries
        elif(type(data) == dict):

            if(len(data) == 0 or all(value is None or not value for value in data.values())):
                datatypes["Empty dict"] += 1
            else:
                for index, value in data.items():
                    if(value is not None):
                        datatypes[index] = type(value)

    # printing data types details
    print(f"\n{column.name}")
    for index, val in datatypes.items():
        print(f"{index}: {val}")

df.apply(print_detailed_datatypes)
# Observation: Many NumPy arrays in the dataset are empty
# TO DO: Convert empty arrays and dictionaries to NaN for better processing

# Identifying relevant columns for deduplication
relevant_columns = ['unspsc', 'product_title', 'product_name', 'brand']
for column in relevant_columns:
    cnt = df[column].value_counts()
    removed_count = (cnt - 1).sum() 
    print(f"{column}: {removed_count}")
# Observation: 'product_name' appears to be the most efficient column for deduplication

# Handling missing product names (36 missing values identified)
def print_relevant_info(row):

    """Check if rows with missing 'product_name' contain any relevant alternative information."""

    if pd.isna(row["product_name"]):
        relevant_info = set()
        print(f"{row.name} : ", end="")

        for column in df.columns:
            if isinstance(row[column], np.ndarray):
                if row[column].size > 0:
                    relevant_info.add(column)
            elif pd.notna(row[column]):
                relevant_info.add(column)

        print(relevant_info)

df.apply(print_relevant_info, axis = 1)
# Observation: Some products not consist of any meaningful data. No special handling is necessary due to their insignificance