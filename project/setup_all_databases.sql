-- ===========================================
-- MASTER DATABASE & TABLE SETUP SCRIPT
-- Creates:
--   1. users_db
--   2. products_db
--   3. reports_db
-- With tables and sample data
-- ===========================================

-- =========================
-- 1️⃣ USERS DATABASE
-- =========================
DROP DATABASE IF EXISTS users_db;
CREATE DATABASE users_db;

\c users_db;

CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(15),
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE user_orders (
    order_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    order_amount DECIMAL(10,2),
    order_status VARCHAR(30),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO users (full_name, email, phone, address) VALUES
('Ravi Kumar', 'ravi@gmail.com', '9876543210', 'Hyderabad'),
('Anjali Sharma', 'anjali@gmail.com', '9123456780', 'Delhi');

INSERT INTO user_orders (user_id, order_amount, order_status) VALUES
(1, 2500.00, 'DELIVERED'),
(2, 3800.50, 'PENDING');

-- =========================
-- 2️⃣ PRODUCTS DATABASE
-- =========================
DROP DATABASE IF EXISTS products_db;
CREATE DATABASE products_db;

\c products_db;

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10,2),
    stock INT
);

CREATE TABLE suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(100),
    contact_email VARCHAR(100)
);

INSERT INTO suppliers (supplier_name, contact_email) VALUES
('Tech Supplies Ltd', 'tech@supplier.com'),
('Office Goods Inc', 'office@supplier.com');

INSERT INTO products (product_name, category, price, stock) VALUES
('Laptop', 'Electronics', 55000.00, 30),
('Printer', 'Office', 12000.00, 12),
('Chair', 'Furniture', 3500.00, 40);

-- =========================
-- 3️⃣ REPORTS DATABASE
-- =========================
DROP DATABASE IF EXISTS reports_db;
CREATE DATABASE reports_db;

\c reports_db;

CREATE TABLE daily_sales (
    sale_id SERIAL PRIMARY KEY,
    sale_date DATE,
    total_orders INT,
    total_revenue DECIMAL(12,2)
);

CREATE TABLE traffic_stats (
    stat_id SERIAL PRIMARY KEY,
    visit_date DATE,
    page_views INT,
    unique_visitors INT
);

INSERT INTO daily_sales (sale_date, total_orders, total_revenue) VALUES
('2025-12-08', 20, 58000.00),
('2025-12-09', 32, 96400.50);

INSERT INTO traffic_stats (visit_date, page_views, unique_visitors) VALUES
('2025-12-08', 1500, 420),
('2025-12-09', 2100, 610);

-- =========================
-- ✅ DONE
-- =========================
SELECT 'ALL DATABASES & TABLES CREATED SUCCESSFULLY!' AS status;
