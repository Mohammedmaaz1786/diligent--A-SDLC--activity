SELECT
    c.customer_id,
    c.full_name,
    COALESCE(COUNT(DISTINCT o.order_id), 0) AS total_orders,
    COALESCE(SUM(oi.quantity), 0) AS total_quantity,
    COALESCE(SUM(oi.quantity * oi.unit_price), 0) AS total_revenue
FROM
    customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY
    c.customer_id,
    c.full_name
ORDER BY
    total_revenue DESC;
