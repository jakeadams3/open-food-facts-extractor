# Open Food Facts Data Extractor

A simple Python tool to extract specific fields from the Open Food Facts dataset.

## Features

- Extracts product code, ingredients, and product name from Open Food Facts parquet files
- Memory-efficient processing for large datasets
- Removes duplicates and cleans up data format
- Exports to CSV format

## Requirements

- Python 3.6+
- Dependencies listed in `requirements.txt`

## Usage

1. Place your `food.parquet` file in the same directory
2. Run `python data_extract.py`
3. The extracted data will be saved to `food_extracted.csv`

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