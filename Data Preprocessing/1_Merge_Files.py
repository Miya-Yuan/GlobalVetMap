import pandas as pd
import re
from difflib import get_close_matches

# List of CSV file paths
csv_file_paths = [
    "C:/Users/myuan/Desktop/CHE/OSM/CHE_VP_OSM.csv",
    "C:/Users/myuan/Desktop/CHE/LocalCH/vet_practices.csv",
    "C:/Users/myuan/Desktop/CHE/GST/vet_practices.csv",
    # Add more paths
]

# Target columns with correct names
target_columns = ["Name", "Address", "Email", "Website", "Specialization", "Category", "Latitude", "Longitude"]

# Normalize function for column names
def normalize(col_name):
    return re.sub(r'[^a-z0-9]', '', col_name.strip().lower())

# Mapping from normalized form to original column name
normalized_target = {normalize(col): col for col in target_columns}

df_list = []

for file_path in csv_file_paths:
    try:
        df = pd.read_csv(file_path)
        original_len = len(df)
        print(f"Read {original_len} rows from: {file_path}")

        # Build normalized map of current file
        normalized_map = {normalize(col): col for col in df.columns}

        # Try to match normalized columns to the target
        matched_cols = {}
        for norm_col, orig_col in normalized_map.items():
            match = get_close_matches(norm_col, normalized_target.keys(), n=1, cutoff=0.85)
            if match:
                matched_cols[normalized_target[match[0]]] = orig_col

        # Create an empty DataFrame with all target columns
        cleaned_df = pd.DataFrame(columns=target_columns)
        # Fill in matched columns
        for target_col in target_columns:
            if target_col in matched_cols:
                cleaned_df[target_col] = df[matched_cols[target_col]]
            else:
                cleaned_df[target_col] = ""  # fill missing with empty string
        
        cleaned_len = len(cleaned_df)
        print(f"Cleaned to {cleaned_len} rows with standardized columns.")
        df_list.append(cleaned_df)

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

# Concatenate all cleaned DataFrames
merged_df = pd.concat(df_list, ignore_index=True)
print(f"\nTotal rows after concatenation: {len(merged_df)}")
# Drop duplicates based on the "Name" column
merged_df.drop_duplicates(subset=["Name"], inplace=True)
print(f"Total rows after dropping duplicates on 'Name': {len(merged_df)}")
# Save the merged DataFrame
merged_df.to_csv("C:/Users/myuan/Desktop/CHE/merged_output.csv", index=False)
print("✅ Merged CSV saved!")
