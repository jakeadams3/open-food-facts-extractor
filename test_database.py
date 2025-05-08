import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, db

# Load environment variables
load_dotenv()

# Initialize Firebase
cred = credentials.Certificate(os.environ.get('FIREBASE_SERVICE_ACCOUNT_PATH', 'serviceAccountKey.json'))
firebase_admin.initialize_app(cred, {
    'databaseURL': os.environ.get('FIREBASE_DATABASE_URL')
})

def test_product_lookup(barcode):
    print(f"Looking up barcode: {barcode}")
    
    # Get reference to the product
    product_ref = db.reference(f'products/{barcode}')
    
    # Retrieve the product data
    product = product_ref.get()
    
    if product:
        print("Product found!")
        print(f"Name: {product.get('name', 'N/A')}")
        print(f"Ingredients: {product.get('ingredients', 'N/A')}")
        print(f"Code: {product.get('code', 'N/A')}")
    else:
        print(f"No product found with barcode {barcode}")

# Test with a few barcodes from your sample
test_codes = [
    '101209159',  # Véritable pâte à tartiner
    '105000011',  # Chamomile Herbal Tea
    '3017620422003',  # Nutella (a common product to try)
    '92000111107153600155',
    '830028000931',
    '0810014672342',
    '0049000002485',
    '0009800820023'
]

for code in test_codes:
    test_product_lookup(code)
    print("-" * 50)