# Open Food Facts Data Extractor

A simple Python tool to extract specific fields from the Open Food Facts dataset and upload them to Firebase.

## Features

- Extracts product code, ingredients, and product name from Open Food Facts parquet files
- Memory-efficient processing for large datasets
- Removes duplicates and cleans up data format
- Exports to CSV format
- Uploads product data to Firebase Realtime Database
- Provides testing tools to verify database uploads

## Requirements

- Python 3.6+
- Dependencies listed in `requirements.txt`
- Firebase Realtime Database account

## Usage

### Data Extraction

1. Place your `food.parquet` file in the same directory
2. Run `python data_extract.py`
3. The extracted data will be saved to `food_extracted.csv`

### Firebase Upload

1. Create a `.env` file with the following variables:
   ```
   FIREBASE_SERVICE_ACCOUNT_PATH=serviceAccountKey.json
   FIREBASE_DATABASE_URL=https://your-project-id.firebaseio.com
   CSV_FILE=food_extracted.csv
   UPLOAD_CHUNK_SIZE=5000
   MAX_RETRIES=3
   ```
2. Place your Firebase service account key JSON file in the same directory
3. Run `python firebase_upload.py`
4. The data will be uploaded to your Firebase Realtime Database

### Testing Database

1. After uploading, you can test the database by running:
   ```
   python test_database.py
   ```
2. This will look up a few sample barcodes to verify the data was uploaded correctly

## Installation

```bash
# Clone this repository
git clone https://github.com/yourusername/open-food-facts-extractor.git

# Navigate to the project directory
cd open-food-facts-extractor

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Firebase Setup

1. Create a Firebase project at [console.firebase.google.com](https://console.firebase.google.com)
2. Go to Project Settings > Service accounts
3. Generate a new private key and save it as `serviceAccountKey.json` in your project directory
4. Enable the Realtime Database in your Firebase project
5. Copy the database URL for your `.env` file