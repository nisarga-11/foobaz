-- ===============================================
-- PostgreSQL PG1 Setup Script
-- Server: localhost:5432 (Default PostgreSQL)
-- MCP Server: http://localhost:8001
-- Databases: customer_db, inventory_db, analytics_db
-- ===============================================

-- Connect as superuser to create databases
-- Run: psql -U postgres -h localhost -p 5432

-- Create databases
CREATE DATABASE customer_db WITH ENCODING 'UTF8';
CREATE DATABASE inventory_db WITH ENCODING 'UTF8';
CREATE DATABASE analytics_db WITH ENCODING 'UTF8';

-- ===============================================
-- customer_db Setup
-- ===============================================
\c customer_db;

-- Create customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create orders table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    shipping_address TEXT,
    CONSTRAINT valid_status CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled'))
);

-- Create order_items table
CREATE TABLE order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_name VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(8,2) NOT NULL CHECK (unit_price >= 0),
    line_total DECIMAL(10,2) NOT NULL CHECK (line_total >= 0)
);

-- Insert sample customers
INSERT INTO customers (first_name, last_name, email, phone) VALUES
('John', 'Doe', 'john.doe@email.com', '+1-555-0123'),
('Jane', 'Smith', 'jane.smith@email.com', '+1-555-0124'),
('Bob', 'Johnson', 'bob.johnson@email.com', '+1-555-0125'),
('Alice', 'Williams', 'alice.williams@email.com', '+1-555-0126'),
('Charlie', 'Brown', 'charlie.brown@email.com', '+1-555-0127'),
('Diana', 'Davis', 'diana.davis@email.com', '+1-555-0128'),
('Edward', 'Miller', 'edward.miller@email.com', '+1-555-0129'),
('Fiona', 'Wilson', 'fiona.wilson@email.com', '+1-555-0130');

-- Insert sample orders
INSERT INTO orders (customer_id, total_amount, status, shipping_address) VALUES
(1, 299.99, 'completed', '123 Main St, Anytown, USA'),
(2, 149.50, 'pending', '456 Oak Ave, Another City, USA'),
(1, 89.99, 'shipped', '123 Main St, Anytown, USA'),
(3, 549.99, 'processing', '789 Pine St, Somewhere, USA'),
(4, 79.99, 'delivered', '321 Elm St, Elsewhere, USA'),
(2, 199.99, 'completed', '456 Oak Ave, Another City, USA'),
(5, 349.99, 'shipped', '654 Maple Ave, Anywhere, USA'),
(6, 129.99, 'pending', '987 Cedar St, Nowhere, USA');

-- Insert sample order items
INSERT INTO order_items (order_id, product_name, quantity, unit_price, line_total) VALUES
(1, 'Business Laptop', 1, 299.99, 299.99),
(2, 'Wireless Mouse', 2, 29.99, 59.98),
(2, 'USB Cable', 3, 9.99, 29.97),
(3, 'Desk Lamp', 1, 89.99, 89.99),
(4, 'Monitor Stand', 1, 149.99, 149.99),
(4, 'Keyboard', 2, 199.99, 399.98),
(5, 'Webcam', 1, 79.99, 79.99),
(6, 'Headphones', 1, 199.99, 199.99),
(7, 'Standing Desk', 1, 349.99, 349.99),
(8, 'Office Chair', 1, 129.99, 129.99);

-- Create indexes for better performance
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);

-- ===============================================
-- inventory_db Setup
-- ===============================================
\c inventory_db;

-- Create products table
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    description TEXT,
    category VARCHAR(50),
    unit_price DECIMAL(8,2) NOT NULL CHECK (unit_price >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create inventory table
CREATE TABLE inventory (
    inventory_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    warehouse_location VARCHAR(50) NOT NULL,
    quantity_on_hand INTEGER NOT NULL DEFAULT 0 CHECK (quantity_on_hand >= 0),
    reorder_level INTEGER NOT NULL DEFAULT 10 CHECK (reorder_level >= 0),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create inventory_movements table
CREATE TABLE inventory_movements (
    movement_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    movement_type VARCHAR(20) NOT NULL,
    quantity_change INTEGER NOT NULL,
    movement_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reference_number VARCHAR(50),
    notes TEXT,
    CONSTRAINT valid_movement_type CHECK (movement_type IN ('inbound', 'outbound', 'adjustment'))
);

-- Insert sample products
INSERT INTO products (sku, product_name, description, category, unit_price) VALUES
('LAP-001', 'Business Laptop', '15-inch business laptop with SSD', 'Electronics', 899.99),
('MOU-001', 'Wireless Mouse', 'Ergonomic wireless mouse', 'Electronics', 29.99),
('DES-001', 'Standing Desk', 'Adjustable height standing desk', 'Furniture', 349.99),
('MON-001', 'Monitor 24inch', '24-inch LED monitor', 'Electronics', 199.99),
('KEY-001', 'Mechanical Keyboard', 'RGB mechanical keyboard', 'Electronics', 129.99),
('CHR-001', 'Office Chair', 'Ergonomic office chair', 'Furniture', 299.99),
('CAM-001', 'Webcam HD', '1080p HD webcam', 'Electronics', 79.99),
('HED-001', 'Noise Cancelling Headphones', 'Wireless noise cancelling headphones', 'Electronics', 199.99);

-- Insert sample inventory
INSERT INTO inventory (product_id, warehouse_location, quantity_on_hand, reorder_level) VALUES
(1, 'Warehouse-A', 45, 10),
(2, 'Warehouse-A', 120, 25),
(3, 'Warehouse-B', 8, 5),
(4, 'Warehouse-A', 32, 15),
(5, 'Warehouse-A', 67, 20),
(6, 'Warehouse-B', 15, 8),
(7, 'Warehouse-A', 88, 30),
(8, 'Warehouse-A', 42, 12);

-- Insert sample inventory movements
INSERT INTO inventory_movements (product_id, movement_type, quantity_change, reference_number, notes) VALUES
(1, 'inbound', 50, 'PO-2025-001', 'Received shipment from supplier'),
(2, 'outbound', -25, 'SO-2025-015', 'Sold to customer order'),
(3, 'adjustment', -2, 'ADJ-2025-003', 'Damaged units removed'),
(4, 'inbound', 40, 'PO-2025-002', 'New stock arrival'),
(5, 'outbound', -15, 'SO-2025-016', 'Bulk order shipment'),
(6, 'inbound', 20, 'PO-2025-003', 'Restocking furniture'),
(7, 'outbound', -12, 'SO-2025-017', 'Corporate order'),
(8, 'adjustment', 5, 'ADJ-2025-004', 'Inventory count correction');

-- Create indexes
CREATE INDEX idx_products_sku ON products(sku);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_inventory_product_id ON inventory(product_id);
CREATE INDEX idx_inventory_warehouse ON inventory(warehouse_location);
CREATE INDEX idx_movements_product_id ON inventory_movements(product_id);
CREATE INDEX idx_movements_date ON inventory_movements(movement_date);

-- ===============================================
-- analytics_db Setup
-- ===============================================
\c analytics_db;

-- Create sales_metrics table
CREATE TABLE sales_metrics (
    metric_id SERIAL PRIMARY KEY,
    date_recorded DATE NOT NULL,
    total_sales DECIMAL(12,2) NOT NULL CHECK (total_sales >= 0),
    order_count INTEGER NOT NULL CHECK (order_count >= 0),
    avg_order_value DECIMAL(8,2) NOT NULL CHECK (avg_order_value >= 0),
    top_selling_category VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create customer_analytics table
CREATE TABLE customer_analytics (
    analytics_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    total_lifetime_value DECIMAL(10,2) NOT NULL CHECK (total_lifetime_value >= 0),
    order_frequency DECIMAL(4,2) CHECK (order_frequency >= 0),
    last_order_date DATE,
    customer_segment VARCHAR(30),
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_segment CHECK (customer_segment IN ('high_value', 'regular', 'at_risk', 'new'))
);

-- Create product_analytics table
CREATE TABLE product_analytics (
    product_analytics_id SERIAL PRIMARY KEY,
    product_sku VARCHAR(50) NOT NULL,
    total_units_sold INTEGER DEFAULT 0,
    total_revenue DECIMAL(12,2) DEFAULT 0.00,
    avg_selling_price DECIMAL(8,2),
    last_sale_date DATE,
    trend VARCHAR(20), -- 'increasing', 'stable', 'decreasing'
    analysis_date DATE NOT NULL DEFAULT CURRENT_DATE
);

-- Insert sample sales metrics
INSERT INTO sales_metrics (date_recorded, total_sales, order_count, avg_order_value, top_selling_category) VALUES
('2025-09-01', 2450.75, 12, 204.23, 'Electronics'),
('2025-09-02', 1890.50, 9, 210.06, 'Electronics'),
('2025-09-03', 3200.25, 15, 213.35, 'Furniture'),
('2025-09-04', 1650.00, 8, 206.25, 'Electronics'),
('2025-09-05', 2890.75, 14, 206.48, 'Electronics'),
('2025-09-06', 2150.50, 11, 195.50, 'Electronics'),
('2025-09-07', 1750.25, 7, 250.04, 'Furniture'),
('2025-09-08', 2950.00, 13, 226.92, 'Electronics'),
('2025-09-09', 2100.75, 10, 210.08, 'Electronics'),
('2025-09-10', 3450.50, 16, 215.66, 'Electronics');

-- Insert sample customer analytics
INSERT INTO customer_analytics (customer_id, total_lifetime_value, order_frequency, last_order_date, customer_segment) VALUES
(1, 389.98, 2.5, '2025-09-10', 'regular'),
(2, 349.49, 1.8, '2025-09-08', 'regular'),
(3, 549.99, 1.2, '2025-09-07', 'high_value'),
(4, 79.99, 0.5, '2025-09-05', 'new'),
(5, 349.99, 1.0, '2025-09-09', 'regular'),
(6, 129.99, 0.8, '2025-09-06', 'new'),
(7, 0.00, 0.0, NULL, 'new'),
(8, 0.00, 0.0, NULL, 'new');

-- Insert sample product analytics
INSERT INTO product_analytics (product_sku, total_units_sold, total_revenue, avg_selling_price, last_sale_date, trend) VALUES
('LAP-001', 15, 13499.85, 899.99, '2025-09-10', 'stable'),
('MOU-001', 45, 1349.55, 29.99, '2025-09-09', 'increasing'),
('DES-001', 8, 2799.92, 349.99, '2025-09-08', 'stable'),
('MON-001', 12, 2399.88, 199.99, '2025-09-07', 'increasing'),
('KEY-001', 18, 2339.82, 129.99, '2025-09-09', 'stable'),
('CHR-001', 5, 1499.95, 299.99, '2025-09-06', 'decreasing'),
('CAM-001', 22, 1759.78, 79.99, '2025-09-10', 'increasing'),
('HED-001', 9, 1799.91, 199.99, '2025-09-08', 'stable');

-- Create indexes
CREATE INDEX idx_sales_metrics_date ON sales_metrics(date_recorded);
CREATE INDEX idx_customer_analytics_customer_id ON customer_analytics(customer_id);
CREATE INDEX idx_customer_analytics_segment ON customer_analytics(customer_segment);
CREATE INDEX idx_product_analytics_sku ON product_analytics(product_sku);
CREATE INDEX idx_product_analytics_date ON product_analytics(analysis_date);

-- Grant permissions (adjust as needed for your environment)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO backup_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO backup_user;

ECHO 'PG1 databases setup complete!';
ECHO 'Databases created: customer_db, inventory_db, analytics_db';
ECHO 'Total tables: 9';
ECHO 'Sample data inserted for testing backup/restore operations';
