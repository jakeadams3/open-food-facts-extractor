import pandas as pd
import pyarrow.parquet as pq
import os
import time
import csv
import numpy as np
import json
import re

# Check if the parquet file exists
parquet_file_path = 'food.parquet'
if not os.path.exists(parquet_file_path):
    print(f"Error: {parquet_file_path} not found!")
    print(f"Make sure to place it in this directory: {os.getcwd()}")
    exit(1)

print(f"Starting to process {parquet_file_path}...")
start_time = time.time()

try:
    # Output file
    output_file = 'food_extracted.csv'
    
    # Get the schema to find available columns
    parquet_schema = pq.read_schema(parquet_file_path)
    column_names = parquet_schema.names
    
    # Determine which columns to read
    col_map = {}
    if 'code' in column_names:
        col_map['code'] = 'code'
    elif '_id' in column_names:
        col_map['_id'] = 'code'
    
    if 'ingredients_text' in column_names:
        col_map['ingredients_text'] = 'ingredients_text'
    elif 'ingredients_text_en' in column_names:
        col_map['ingredients_text_en'] = 'ingredients_text'
    
    if 'product_name' in column_names:
        col_map['product_name'] = 'product_name'
    elif 'product_name_en' in column_names:
        col_map['product_name_en'] = 'product_name'
    
    input_columns = list(col_map.keys())
    output_columns = list(col_map.values())
    
    print(f"Reading columns: {input_columns}")
    print(f"Will be saved as: {output_columns}")
    
    # Open the parquet file using pyarrow
    parquet_file = pq.ParquetFile(parquet_file_path)
    
    # Count total rows for reporting
    total_rows = 0
    processed_rows = 0
    unique_codes = set()
    
    # Function to extract text from potential JSON/dict string
    def extract_text(val):
        if pd.isna(val):
            return ""
        
        val_str = str(val)
        
        # Clean up common formatting issues
        val_str = val_str.replace('\\"', '"').replace("\\'", "'")
        
        # Try to parse as JSON if it looks like a dict
        if (val_str.strip().startswith('{') and ('text' in val_str or 'lang' in val_str)):
            try:
                # Replace single quotes with double quotes for JSON parsing
                json_str = val_str.replace("'", '"')
                data = json.loads(json_str)
                if isinstance(data, dict) and 'text' in data:
                    return data['text']
            except json.JSONDecodeError:
                # Regex patterns for different variations
                patterns = [
                    r"'text':\s*'([^']*)'",
                    r"'text':\s*\"([^\"]*)\"",
                    r'"text":\s*"([^"]*)"',
                    r'"text":\s*\'([^\']*)\'',
                    r"\{.*?'text':\s*['\"](.+?)['\"].*?\}",
                    r"\{.*?\"text\":\s*['\"](.+?)['\"].*?\}"
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, val_str)
                    if match:
                        return match.group(1)
                        
                # If we're still here, try a more aggressive approach
                if "text" in val_str:
                    text_parts = val_str.split("text")[1]
                    # Find content between quotes after "text":
                    clean_text = re.search(r"['\"]:\s*['\"](.+?)['\"]", text_parts)
                    if clean_text:
                        return clean_text.group(1)
        
        return val_str
    
    # Function to safely convert value to string
    def safe_str(val):
        if isinstance(val, (list, np.ndarray)):
            if len(val) > 0:
                return extract_text(val[0])
            return ""
        else:
            return extract_text(val)
    
    # Process in chunks and write directly to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=output_columns)
        writer.writeheader()
        
        # Process the file in batches
        rows_per_batch = 10000
        
        for batch in parquet_file.iter_batches(batch_size=rows_per_batch, columns=input_columns):
            # Convert to pandas for easier processing
            chunk_df = batch.to_pandas()
            total_rows += len(chunk_df)
            
            # Process each row
            for _, row in chunk_df.iterrows():
                row_dict = {}
                
                # Check if we have the code column and it's not already seen
                code_val = None
                for src_col, dst_col in col_map.items():
                    if dst_col == 'code' and src_col in row:
                        val = row[src_col]
                        if not pd.isna(val).all() if isinstance(val, (list, np.ndarray)) else not pd.isna(val):
                            code_val = safe_str(val)
                            break
                
                # Skip if no valid code or if we've seen this code before
                if code_val is None or code_val in unique_codes:
                    continue
                
                unique_codes.add(code_val)
                row_dict['code'] = code_val
                
                # Copy other fields
                for src_col, dst_col in col_map.items():
                    if dst_col != 'code' and src_col in row:
                        row_dict[dst_col] = safe_str(row[src_col])
                
                # Write to CSV
                writer.writerow(row_dict)
                processed_rows += 1
            
            # Print progress
            print(f"Processed {total_rows} rows, kept {processed_rows} unique items")
    
    # Display file sizes
    parquet_size = os.path.getsize(parquet_file_path) / (1024 * 1024)  # Size in MB
    csv_size = os.path.getsize(output_file) / (1024 * 1024)  # Size in MB
    print(f"\nOriginal parquet file: {parquet_size:.2f} MB")
    print(f"Extracted CSV file: {csv_size:.2f} MB")
    
    elapsed_time = time.time() - start_time
    print(f"Processing completed in {elapsed_time:.2f} seconds")
    print(f"Total rows processed: {total_rows}")
    print(f"Unique products extracted: {processed_rows}")
    
except Exception as e:
    print(f"Error processing the parquet file: {e}")
    import traceback
    traceback.print_exc()