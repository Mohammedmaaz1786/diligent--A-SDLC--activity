# E-Commerce Analytics Pipeline

## Overview

This project demonstrates an end-to-end e-commerce analytics workflow built using AI-driven development. The pipeline includes synthetic data generation, automated ETL processes, and SQL-based customer analytics.

The project was developed using **structured AI prompts** (stored in `prompts/`) to guide the creation of realistic datasets, ingestion scripts, and reporting tools. This approach showcases how well-crafted prompts can accelerate development while maintaining code quality and consistency.

**Key Components:**
- Synthetic e-commerce datasets (customers, products, orders, order items, payments)
- Automated data ingestion pipeline (CSV to SQLite)
- Customer revenue analytics with formatted reporting
- Complete documentation of the development process via prompts

---

## Directory Structure

```
Diligent/
|
+-- data/                              # Synthetic CSV datasets
|   +-- customers.csv                  # 250 customer records
|   +-- products.csv                   # 250 product records
|   +-- orders.csv                     # 300 order records
|   +-- order_items.csv                # 487 order line items
|   +-- payments.csv                   # 299 payment records
|
+-- database/                          # SQLite database (auto-generated)
|   +-- ecom.db                        # Database created by load_data.py
|
+-- output/                            # Analytics reports
|   +-- customer_revenue_output.csv    # Customer revenue analysis (250 rows)
|
+-- prompts/                           # AI prompts documenting development
|   +-- prompt1_generate_data.md       # Data generation prompt
|   +-- prompt2_load_data.md           # ETL script creation prompt
|   +-- prompt3_sql_report.md          # Analytics script creation prompt
|
+-- load_data.py                       # ETL pipeline script
+-- customer_revenue.sql               # SQL analytics query
+-- run_query.py                       # Query executor and reporting script
+-- README.md                          # This file
```

### Folder & File Descriptions

**data/**  
Contains 5 synthetic CSV files with realistic e-commerce data. All data is fully synthetic (non-PII) with proper referential integrity maintained across tables.

**database/**  
Auto-generated SQLite database (`ecom.db`) created by running `load_data.py`. Contains 5 tables matching the CSV structure.

**output/**  
Analytics output files. The `customer_revenue_output.csv` file contains customer-level revenue metrics sorted by total revenue.

**prompts/**  
Documentation of AI prompts used to build each component. Demonstrates the agentic SDLC workflow where structured prompts guide autonomous development.

**load_data.py**  
ETL pipeline that reads CSV files from `data/`, validates and cleans data, then loads it into SQLite. Includes error handling and progress logging.

**customer_revenue.sql**  
SQL query that joins customers, orders, and order_items to calculate total orders, quantities, and revenue per customer. Uses COALESCE for NULL handling.

**run_query.py**  
Executes the SQL query, generates formatted terminal output, and exports results to CSV. Includes summary statistics and top customer rankings.

---

## Prompts & Development Workflow

The `prompts/` folder documents how each component was built using AI-assisted development:

### prompt1_generate_data.md
**Purpose:** Generate synthetic e-commerce datasets  
**Output:** 5 CSV files (customers, products, orders, order_items, payments)  
**Key Requirements:** Referential integrity, realistic Indian names/cities, varied product categories

### prompt2_load_data.md
**Purpose:** Create automated ETL pipeline  
**Output:** `load_data.py` script  
**Key Features:** CSV validation, data cleaning, SQLite integration, ASCII-safe logging

### prompt3_sql_report.md
**Purpose:** Build analytics query and reporting tool  
**Output:** `customer_revenue.sql` + `run_query.py`  
**Key Features:** Multi-table JOINs, NULL handling, formatted output, CSV export

This workflow demonstrates how structured prompts enable rapid, iterative development while maintaining full traceability of design decisions.

---

## How to Run

### Prerequisites

Install required Python packages:
```
pip install pandas
```

### Step 1: Load Data into Database

Run the ETL pipeline to create the database:
```
python load_data.py
```

This will:
- Read all CSV files from `data/`
- Validate and clean the data
- Create `database/ecom.db`
- Load data into 5 tables
- Print row counts for verification

### Step 2: Generate Analytics Report

Execute the analytics script:
```
python run_query.py
```

This will:
- Connect to `database/ecom.db`
- Execute `customer_revenue.sql`
- Save results to `output/customer_revenue_output.csv`
- Display formatted table with top 10 customers
- Print summary statistics (total revenue, active/inactive customers)

### Step 3: Review Results

View the output CSV:
```
cat output/customer_revenue_output.csv
```

Or open it in Excel, Google Sheets, or any spreadsheet application.

---

## Technical Stack

- **Python 3.13** - Core scripting language
- **pandas** - Data manipulation and CSV I/O
- **sqlite3** - Embedded database
- **tabulate** - Table formatting (optional)
- **Windows PowerShell** - Execution environment

---

## Data Overview

- **Total Records:** 1,486 across 5 tables
- **Customers:** 250 (220 active, 30 inactive)
- **Products:** 250 across 20+ categories
- **Orders:** 300 with full payment coverage
- **Total Revenue:** ~Rs.8,995,179

All data maintains referential integrity. Inactive customers (no orders) are properly handled with zero values in analytics output.

---

## License

This project is provided as-is for educational and demonstration purposes.
