import pandas as pd
import firebase_admin
from firebase_admin import credentials, db
import time
import os
import sys
from urllib.error import URLError
import requests.exceptions
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# File path 
csv_file = os.environ.get('CSV_FILE', 'food_extracted.csv')

def upload_to_realtime_db():
    start_time = time.time()
    
    # Verify file exists
    if not os.path.exists(csv_file):
        print(f"Error: CSV file '{csv_file}' not found in {os.getcwd()}")
        return False
        
    # Get service account key path from environment variable
    service_account_path = os.environ.get('FIREBASE_SERVICE_ACCOUNT_PATH', 'serviceAccountKey.json')
    
    # Verify service account key exists
    if not os.path.exists(service_account_path):
        print(f"Error: Service account key file '{service_account_path}' not found in {os.getcwd()}")
        return False
    
    try:
        # Initialize Firebase using environment variables
        cred = credentials.Certificate(service_account_path)
        firebase_admin.initialize_app(cred, {
            'databaseURL': os.environ.get('FIREBASE_DATABASE_URL')
        })
        
        # Verify connection by writing a test value
        test_ref = db.reference('connection_test')
        test_ref.set({"timestamp": int(time.time())})
        test_ref.delete()
        print("Firebase connection verified successfully")
        
    except (ValueError, URLError, requests.exceptions.RequestException) as e:
        print(f"Firebase connection error: {e}")
        print("Please check your service account key and database URL in your .env file")
        return False
    
    # Get file size for reporting
    file_size_mb = os.path.getsize(csv_file) / (1024 * 1024)
    print(f"Starting upload of {file_size_mb:.2f}MB CSV file...")
    
    # Count total rows for progress tracking
    total_rows = sum(1 for _ in open(csv_file)) - 1  # Subtract 1 for header
    print(f"Found {total_rows} products to upload")
    
    # Read and upload in manageable chunks
    chunk_size = int(os.environ.get('UPLOAD_CHUNK_SIZE', 5000))  # Configurable chunk size
    uploaded_count = 0
    
    ref = db.reference('products')
    
    try:
        for chunk_num, chunk in enumerate(pd.read_csv(csv_file, chunksize=chunk_size)):
            print(f"Processing chunk {chunk_num+1}...")
            
            # Create batch update dictionary
            batch_update = {}
            for _, row in chunk.iterrows():
                barcode = str(row['code']).strip()
                if not barcode:
                    continue
                    
                # Create product data with code field included
                batch_update[barcode] = {
                    'code': barcode,  # Added the code field as requested
                    'name': str(row['product_name']) if not pd.isna(row['product_name']) else '',
                    'ingredients': str(row['ingredients_text']) if not pd.isna(row['ingredients_text']) else ''
                }
            
            # Upload this chunk with retry logic
            success = False
            max_retries = int(os.environ.get('MAX_RETRIES', 3))
            for attempt in range(max_retries):
                try:
                    ref.update(batch_update)
                    success = True
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt  # Exponential backoff
                        print(f"Upload failed, retrying in {wait_time} seconds... ({str(e)})")
                        time.sleep(wait_time)
                    else:
                        print(f"Failed to upload chunk after {max_retries} attempts: {e}")
                        return False
            
            if success:
                uploaded_count += len(batch_update)
                print(f"Uploaded {len(batch_update)} products (Total: {uploaded_count}/{total_rows}, {uploaded_count/total_rows*100:.2f}%)")
            else:
                print("Upload failed for unknown reason")
                return False
        
        elapsed_time = time.time() - start_time
        print(f"Upload completed in {elapsed_time:.2f} seconds")
        print(f"Total products uploaded: {uploaded_count}")
        return True
        
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False
    
if __name__ == "__main__":
    print("Starting product database upload script...")
    success = upload_to_realtime_db()
    if success:
        print("Script completed successfully!")
    else:
        print("Script failed! See error messages above.")
        sys.exit(1)