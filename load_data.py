import os
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime

def create_database_folder():
    """Create database folder if it doesn't exist."""
    db_folder = Path("database")
    db_folder.mkdir(exist_ok=True)
    return db_folder

def validate_columns(df, table_name, required_columns):
    """Validate that all required columns exist in the dataframe."""
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(
            f"Missing columns in {table_name}: {', '.join(missing_columns)}"
        )
    print(f"[OK] {table_name}: All required columns present")

def clean_dataframe(df):
    """Perform basic data cleaning on the dataframe."""
    # Trim whitespace from string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip() if df[col].dtype == 'object' else df[col]
    
    # Normalize column names (lowercase, remove extra spaces)
    df.columns = df.columns.str.lower().str.strip()
    
    return df

def convert_dates(df, date_columns):
    """Convert specified columns to datetime format."""
    for col in date_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception as e:
                print(f"  [WARN] Could not convert {col} to datetime: {e}")
    return df

def coerce_numeric(df, numeric_columns):
    """Coerce specified columns to numeric values."""
    for col in numeric_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except Exception as e:
                print(f"  [WARN] Could not convert {col} to numeric: {e}")
    return df

def load_csv_files(data_folder):
    """Load all CSV files from the data folder."""
    print("\n" + "="*60)
    print("LOADING CSV FILES")
    print("="*60)
    
    dataframes = {}
    csv_files = {
        'customers': ['customer_id', 'full_name', 'email', 'phone', 'city', 'created_at'],
        'products': ['product_id', 'product_name', 'category', 'price', 'stock_qty'],
        'orders': ['order_id', 'customer_id', 'order_date', 'status', 'total_amount'],
        'order_items': ['item_id', 'order_id', 'product_id', 'quantity', 'unit_price'],
        'payments': ['payment_id', 'order_id', 'payment_method', 'payment_date', 'amount']
    }
    
    for table_name, required_cols in csv_files.items():
        try:
            file_path = Path(data_folder) / f"{table_name}.csv"
            
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            print(f"\nLoading {table_name}.csv...")
            df = pd.read_csv(file_path)
            
            # Validate columns
            validate_columns(df, table_name, required_cols)
            
            # Clean data
            df = clean_dataframe(df)
            
            # Convert dates for specific tables
            if table_name == 'customers':
                df = convert_dates(df, ['created_at'])
            elif table_name == 'orders':
                df = convert_dates(df, ['order_date'])
            elif table_name == 'payments':
                df = convert_dates(df, ['payment_date'])
            
            # Coerce numeric columns
            numeric_mapping = {
                'customers': ['customer_id', 'phone'],
                'products': ['product_id', 'price', 'stock_qty'],
                'orders': ['order_id', 'customer_id', 'total_amount'],
                'order_items': ['item_id', 'order_id', 'product_id', 'quantity', 'unit_price'],
                'payments': ['payment_id', 'order_id', 'amount']
            }
            df = coerce_numeric(df, numeric_mapping.get(table_name, []))
            
            dataframes[table_name] = df
            print(f"  Loaded {len(df)} rows")
            
        except FileNotFoundError as e:
            print(f"  [ERR] {e}")
            raise
        except ValueError as e:
            print(f"  [ERR] {e}")
            raise
        except Exception as e:
            print(f"  [ERR] Failed to load {table_name}: {e}")
            raise
    
    return dataframes

def create_database(db_path):
    """Create SQLite database connection."""
    print("\n" + "="*60)
    print("CREATING DATABASE")
    print("="*60)
    
    try:
        conn = sqlite3.connect(db_path)
        print(f"[OK] Database created at: {db_path}")
        return conn
    except Exception as e:
        print(f"[ERR] Failed to create database: {e}")
        raise

def insert_data_to_database(conn, dataframes):
    """Insert dataframes into SQLite database."""
    print("\n" + "="*60)
    print("INSERTING DATA INTO DATABASE")
    print("="*60)
    
    try:
        for table_name, df in dataframes.items():
            try:
                print(f"\nInserting {table_name}...")
                df.to_sql(table_name, conn, if_exists='replace', index=False)
                print(f"  [OK] {table_name} inserted successfully")
            except Exception as e:
                print(f"  [ERR] Failed to insert {table_name}: {e}")
                raise
        
        conn.commit()
        print("\n[OK] All data committed to database")
        
    except Exception as e:
        conn.rollback()
        print(f"[ERR] Transaction rolled back: {e}")
        raise

def print_row_counts(conn):
    """Print row counts for all tables."""
    print("\n" + "="*60)
    print("TABLE ROW COUNTS")
    print("="*60)
    
    try:
        cursor = conn.cursor()
        tables = ['customers', 'products', 'orders', 'order_items', 'payments']
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  {table:20} : {count:,} rows")
            except Exception as e:
                print(f"  {table:20} : [ERR] {e}")
        
    except Exception as e:
        print(f"[ERR] Failed to get row counts: {e}")

def main():
    """Main function to orchestrate the entire data loading process."""
    print("\n")
    print("+" + "="*58 + "+")
    print("|" + " "*15 + "E-COMMERCE DATA LOADING SCRIPT" + " "*13 + "|")
    print("+" + "="*58 + "+")
    
    try:
        # Create database folder
        db_folder = create_database_folder()
        print(f"\n[OK] Database folder ready: {db_folder.absolute()}")
        
        # Define paths
        data_folder = "data"
        db_path = db_folder / "ecom.db"
        
        # Verify data folder exists
        if not Path(data_folder).exists():
            raise FileNotFoundError(f"Data folder not found: {data_folder}")
        
        print(f"[OK] Data folder found: {Path(data_folder).absolute()}")
        
        # Load CSV files
        dataframes = load_csv_files(data_folder)
        print(f"\n[OK] Successfully loaded {len(dataframes)} CSV files")
        
        # Create database
        conn = create_database(db_path)
        
        # Insert data
        insert_data_to_database(conn, dataframes)
        
        # Print row counts
        print_row_counts(conn)
        
        # Close connection
        conn.close()
        
        print("\n" + "="*60)
        print("[OK] DATA LOADING COMPLETED SUCCESSFULLY")
        print("="*60)
        print(f"\nDatabase location: {db_path.absolute()}")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("\n")
        
    except FileNotFoundError as e:
        print(f"\n[FATAL] {e}")
        return 1
    except ValueError as e:
        print(f"\n[FATAL] {e}")
        return 1
    except Exception as e:
        print(f"\n[FATAL] Unexpected error occurred: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
