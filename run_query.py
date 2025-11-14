import sqlite3
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Help static type checkers find the symbol without requiring the package at runtime
    from tabulate import tabulate  # type: ignore
else:
    try:
        import importlib
        _tabulate_mod = importlib.import_module("tabulate")
        tabulate = getattr(_tabulate_mod, "tabulate", None)
    except Exception:
        tabulate = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def setup_output_folder():
    """Create output folder if it doesn't exist."""
    output_folder = Path("output")
    output_folder.mkdir(exist_ok=True)
    return output_folder

def read_sql_query(sql_file):
    """Read SQL query from file."""
    try:
        with open(sql_file, 'r') as f:
            query = f.read()
        logging.info(f"SQL query loaded from: {sql_file}")
        return query
    except FileNotFoundError as e:
        logging.error(f"SQL file not found: {sql_file}")
        raise
    except Exception as e:
        logging.error(f"Error reading SQL file: {e}")
        raise

def connect_to_database(db_path):
    """Establish connection to SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        logging.info(f"Connected to database: {db_path}")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        raise

def execute_query(conn, query):
    """Execute SQL query and return results as DataFrame."""
    try:
        df = pd.read_sql_query(query, conn)
        logging.info(f"Query executed successfully. Rows returned: {len(df)}")
        return df
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        raise

def save_to_csv(df, output_path):
    """Save DataFrame to CSV file."""
    try:
        # Replace NaN with 0 for inactive customers
        df['total_orders'] = df['total_orders'].fillna(0).astype(int)
        df['total_quantity'] = df['total_quantity'].fillna(0).astype(int)
        df['total_revenue'] = df['total_revenue'].fillna(0)
        
        df.to_csv(output_path, index=False)
        logging.info(f"Results saved to: {output_path}")
    except Exception as e:
        logging.error(f"Error saving to CSV: {e}")
        raise

def format_currency(value):
    """Format value as currency."""
    if pd.isna(value) or value == 0:
        return "Rs.0.00"
    return f"Rs.{value:,.2f}"

def print_formatted_table(df):
    """Print formatted table using tabulate or pandas."""
    # Filter out customers with zero orders
    active_customers = df[df['total_orders'] > 0].copy()
    inactive_customers = df[df['total_orders'] == 0].copy()
    
    print("\n" + "="*110)
    print("CUSTOMER REVENUE ANALYSIS - ACTIVE CUSTOMERS")
    print("="*110)
    
    if len(active_customers) == 0:
        print("No active customers found.")
        print("="*110 + "\n")
        return
    
    # Format display dataframe for active customers
    display_df = active_customers.copy()
    display_df['total_revenue'] = display_df['total_revenue'].apply(format_currency)
    display_df['total_orders'] = display_df['total_orders'].astype(int)
    display_df['total_quantity'] = display_df['total_quantity'].astype(int)
    
    # Try to use tabulate if available, otherwise use pandas
    if tabulate:
        table_headers = ['Customer ID', 'Full Name', 'Orders', 'Quantity', 'Total Revenue']
        table_data = []
        for idx, row in display_df.iterrows():
            table_data.append([
                int(row['customer_id']),
                row['full_name'],
                row['total_orders'],
                row['total_quantity'],
                row['total_revenue']
            ])
        
        print(tabulate(table_data, headers=table_headers, tablefmt='grid', stralign='left'))
    else:
        # Fallback to formatted printing
        print(f"\n{'ID':<6} {'Full Name':<25} {'Orders':<10} {'Quantity':<12} {'Total Revenue':<20}")
        print("-"*110)
        
        for idx, row in display_df.iterrows():
            print(f"{int(row['customer_id']):<6} {row['full_name']:<25} {row['total_orders']:<10} {row['total_quantity']:<12} {row['total_revenue']:<20}")
    
    print("\n" + "="*110)
    
    # Print summary
    total_active = len(active_customers)
    total_orders = active_customers['total_orders'].sum()
    total_quantity = active_customers['total_quantity'].sum()
    total_revenue = active_customers['total_revenue'].sum()
    avg_revenue = active_customers['total_revenue'].mean()
    
    print("\nSUMMARY STATISTICS - ACTIVE CUSTOMERS")
    print("-"*110)
    print(f"  Active Customers:              {total_active}")
    print(f"  Total Orders:                  {int(total_orders)}")
    print(f"  Total Quantity Sold:           {int(total_quantity)}")
    print(f"  Total Revenue:                 {format_currency(total_revenue)}")
    print(f"  Average Revenue per Customer:  {format_currency(avg_revenue)}")
    
    # Top 10 customers
    print("\n" + "="*110)
    print("TOP 10 CUSTOMERS BY REVENUE")
    print("="*110)
    
    top_10 = active_customers.head(10).copy()
    top_10['total_revenue_fmt'] = top_10['total_revenue'].apply(format_currency)
    top_10['total_orders'] = top_10['total_orders'].astype(int)
    top_10['total_quantity'] = top_10['total_quantity'].astype(int)
    
    if tabulate:
        table_headers = ['Rank', 'Customer ID', 'Full Name', 'Orders', 'Quantity', 'Total Revenue']
        table_data = []
        for rank, (idx, row) in enumerate(top_10.iterrows(), 1):
            table_data.append([
                rank,
                int(row['customer_id']),
                row['full_name'],
                row['total_orders'],
                row['total_quantity'],
                row['total_revenue_fmt']
            ])
        
        print(tabulate(table_data, headers=table_headers, tablefmt='grid', stralign='left'))
    else:
        print(f"\n{'Rank':<6} {'ID':<6} {'Full Name':<25} {'Orders':<10} {'Quantity':<12} {'Total Revenue':<20}")
        print("-"*110)
        
        for rank, (idx, row) in enumerate(top_10.iterrows(), 1):
            print(f"{rank:<6} {int(row['customer_id']):<6} {row['full_name']:<25} {row['total_orders']:<10} {row['total_quantity']:<12} {row['total_revenue_fmt']:<20}")
    
    print("\n" + "="*110)
    
    # Inactive customers summary
    if len(inactive_customers) > 0:
        print("\nINACTIVE CUSTOMERS SUMMARY")
        print("-"*110)
        print(f"  Total Inactive Customers:      {len(inactive_customers)}")
        print(f"  These customers have not placed any orders yet.")
    
    print("\n" + "="*110 + "\n")

def main():
    """Main function to orchestrate the entire process."""
    logging.info("Starting customer revenue analysis...")
    
    try:
        # Setup
        output_folder = setup_output_folder()
        db_path = Path("database") / "ecom.db"
        sql_file = "customer_revenue.sql"
        output_file = output_folder / "customer_revenue_output.csv"
        
        # Verify database exists
        if not db_path.exists():
            raise FileNotFoundError(f"Database not found at: {db_path}")
        
        # Read SQL query
        query = read_sql_query(sql_file)
        
        # Connect to database
        conn = connect_to_database(db_path)
        
        # Execute query
        results_df = execute_query(conn, query)
        
        # Save to CSV
        save_to_csv(results_df, output_file)
        
        # Print formatted table
        print_formatted_table(results_df)
        
        # Close connection
        conn.close()
        logging.info("Database connection closed")
        
        logging.info("Customer revenue analysis completed successfully")
        return 0
        
    except FileNotFoundError as e:
        logging.error(f"[FATAL] {e}")
        return 1
    except Exception as e:
        logging.error(f"[FATAL] Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
