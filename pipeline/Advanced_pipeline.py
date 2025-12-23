import sqlite3
import pandas as pd
import requests
import os
from datetime import datetime

# Configuration
DATA_DIR = "data"
REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)

SQLITE_FILE = os.path.join(DATA_DIR, "anyname.sqlite")
PRODUCTS_FILE = os.path.join(DATA_DIR, "products.csv")
SALES_FILE = os.path.join(DATA_DIR, "sales.csv")
DISCOUNTS_FILE = os.path.join(DATA_DIR, "discounts.txt")
API_URL = "https://dummy.restapiexample.com/api/v1/employees"

# Logging function

def log(message):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}")

# 1. Read SQLite data

log("Connecting to SQLite database...")
try:
    conn = sqlite3.connect(SQLITE_FILE)
    customers = pd.read_sql_query("SELECT * FROM customers", conn)
    conn.close()
    log(f"Loaded {len(customers)} customers from SQLite")
except Exception as e:
    log(f"Error reading SQLite: {e}")
    customers = pd.DataFrame()

# 2. Read CSV files

log("Reading CSV files...")
try:
    products = pd.read_csv(PRODUCTS_FILE)
    sales = pd.read_csv(SALES_FILE)
    log(f"Loaded {len(products)} products and {len(sales)} sales")
except Exception as e:
    log(f"Error reading CSVs: {e}")
    products = pd.DataFrame()
    sales = pd.DataFrame()

# 2. Read CSV files

log("Reading flat file for discounts...")
try:
    discounts = pd.read_csv(DISCOUNTS_FILE, header=None, names=["product_id", "discount_percent"])
    log(f"Loaded {len(discounts)} discount records")
except Exception as e:
    log(f"Error reading flat file: {e}")
    discounts = pd.DataFrame()

# 4. Fetch data from REST API
log("Fetching employee data from API...")
try:
    response = requests.get(API_URL, timeout=10)
    if response.status_code == 200:
        employees = pd.DataFrame(response.json()['data'])
        log(f"Loaded {len(employees)} employees from API")
    else:
        log(f"API returned status code {response.status_code}")
        employees = pd.DataFrame()
except Exception as e:
    log(f"Error fetching API: {e}")
    employees = pd.DataFrame()

# 5. Validate Data
log("Validating data...")
if sales.empty or products.empty:
    log("Critical data missing. Exiting pipeline.")
    exit(1)

# Fill missing discounts with 0
discounts['discount_percent'] = discounts['discount_percent'].fillna(0)

# 6. Merge Data
log("Merging data...")
df = sales.merge(products, on="product_id", how="left")
df = df.merge(discounts, on="product_id", how="left")
if not employees.empty:
    df = df.merge(employees[['id', 'employee_name']], left_on='employee_id', right_on='id', how='left')
else:
    df['employee_name'] = "Unknown"

# 7. Clean and Transform Data
log("Cleaning and transforming data...")
df['discount_percent'] = df['discount_percent'].fillna(0)
df['unit_price'] = df.get('unit_price', 100)  # default price = 100 if missing
df['total_price'] = df['quantity'] * df['unit_price']
df['discounted_price'] = df['total_price'] * (1 - df['discount_percent']/100)

# Convert dates
df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')
df = df.dropna(subset=['sale_date'])  # remove rows with invalid dates

# 8. Analyze Data
log("Performing analysis...")

# Total sales per product
product_sales = df.groupby('product_name').agg(
    total_quantity=('quantity', 'sum'),
    total_sales=('discounted_price', 'sum')
).reset_index()

# Total sales per employee
employee_sales = df.groupby('employee_name').agg(
    total_quantity=('quantity', 'sum'),
    total_sales=('discounted_price', 'sum')
).reset_index()

# Monthly sales trend
df['month'] = df['sale_date'].dt.to_period('M')
monthly_sales = df.groupby('month').agg(
    total_sales=('discounted_price', 'sum')
).reset_index()

# 9. Save Reports
log("Saving reports...")
product_sales.to_csv(os.path.join(REPORT_DIR, "product_sales.csv"), index=False)
employee_sales.to_csv(os.path.join(REPORT_DIR, "employee_sales.csv"), index=False)
monthly_sales.to_csv(os.path.join(REPORT_DIR, "monthly_sales.csv"), index=False)
df.to_csv(os.path.join(REPORT_DIR, "full_sales_data.csv"), index=False)

log("Pipeline completed successfully!")
