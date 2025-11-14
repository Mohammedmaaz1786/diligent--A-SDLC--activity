Generate 5 synthetic e-commerce datasets in CSV format. I need realistic sample data, but fully synthetic and non-PII. Please generate the following files:

1. customers.csv  
   Columns: customer_id, full_name, email, phone, city, created_at

2. products.csv  
   Columns: product_id, product_name, category, price, stock_qty

3. orders.csv  
   Columns: order_id, customer_id, order_date, status, total_amount

4. order_items.csv  
   Columns: item_id, order_id, product_id, quantity, unit_price

5. payments.csv  
   Columns: payment_id, order_id, payment_method, payment_date, amount

Requirements:
- Generate around 200–300 rows per file (order_items can have more).
- Ensure referential integrity (customer_id → orders → order_items).
- Use varied categories, names, and Indian cities.
- Output each dataset as CSV content separately, ready to save as files.
- Do NOT generate code; only produce the CSV data.
