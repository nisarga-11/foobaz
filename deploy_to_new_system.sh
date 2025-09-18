#!/bin/bash

# =============================================================================
# PostgreSQL Backup System - Complete Deployment Script
# This script sets up the entire backup system on a new system
# =============================================================================

set -e

echo "PostgreSQL Backup System - Complete Deployment"
echo "=================================================="

# Simple output without colors

# Default configurations
PG_PORT=${PG_PORT:-5432}
POSTGRES_USER=${POSTGRES_USER:-postgres}
SYSTEM_NAME=${SYSTEM_NAME:-"backup-system"}

echo "Configuration:"
echo "PostgreSQL Server: localhost:$PG_PORT"
echo "PostgreSQL User: $POSTGRES_USER"
echo "System Name: $SYSTEM_NAME"
echo ""

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check if PostgreSQL is running
check_postgres() {
    local port=$1
    
    if pg_isready -h localhost -p $port -U $POSTGRES_USER >/dev/null 2>&1; then
        echo "PostgreSQL (port $port) is running"
        return 0
    else
        echo "PostgreSQL (port $port) is not running"
        return 1
    fi
}

# Function to run SQL file
run_sql_file() {
    local sql_file=$1
    local port=$2
    local description=$3
    
    echo "$description..."
    
    if psql -h localhost -p $port -U $POSTGRES_USER -f "$sql_file" >/dev/null 2>&1; then
        echo "$description completed"
        return 0
    else
        echo "Failed: $description"
        echo "Trying with current user..."
        
        # Try with current user if postgres user fails
        if psql -h localhost -p $port -U $USER -d postgres -f "$sql_file" >/dev/null 2>&1; then
            echo "$description completed (using $USER)"
            return 0
        else
            echo "Failed: $description with both postgres and $USER"
            return 1
        fi
    fi
}

# Step 1: Check Prerequisites
echo "=== Step 1: Checking Prerequisites ==="

# Check if PostgreSQL is installed
if ! command_exists psql; then
    echo "PostgreSQL is not installed"
    echo "To install PostgreSQL:"
    echo "  macOS: brew install postgresql"
    echo "  Ubuntu: sudo apt-get install postgresql postgresql-contrib"
    echo "  CentOS/RHEL: sudo yum install postgresql-server postgresql-contrib"
    exit 1
else
    echo "PostgreSQL is installed"
fi

# Check if Python 3 is installed
if ! command_exists python3; then
    echo "Python 3 is not installed"
    echo "Please install Python 3.8 or higher"
    exit 1
else
    echo "Python 3 is installed"
fi

# Check if PostgreSQL is running
if ! check_postgres $PG_PORT; then
    echo -e "${YELLOW}⚠️  PostgreSQL is not running${NC}"
    echo -e "${BLUE}To start PostgreSQL:${NC}"
    echo "  macOS: brew services start postgresql"
    echo "  Linux: sudo systemctl start postgresql"
    echo ""
    echo "After starting PostgreSQL, run this script again."
    exit 1
fi

echo ""

# Step 2: Create SQL Files
echo -e "${CYAN}=== Step 2: Creating Database Setup Scripts ===${NC}"

# Create sql directory
mkdir -p sql

# Create PG1 setup script
cat > sql/setup_pg1.sql << 'EOF'
-- ===============================================
-- PostgreSQL PG1 Setup Script
-- Databases: customer_db, inventory_db, analytics_db
-- ===============================================

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
EOF

# Create PG2 setup script
cat > sql/setup_pg2.sql << 'EOF'
-- ===============================================
-- PostgreSQL PG2 Setup Script  
-- Databases: hr_db, finance_db, reporting_db
-- ===============================================

-- Create databases
CREATE DATABASE hr_db WITH ENCODING 'UTF8';
CREATE DATABASE finance_db WITH ENCODING 'UTF8';
CREATE DATABASE reporting_db WITH ENCODING 'UTF8';

-- ===============================================
-- hr_db Setup
-- ===============================================
\c hr_db;

-- Create employees table
CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY,
    employee_number VARCHAR(20) UNIQUE NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    department VARCHAR(50) NOT NULL,
    position VARCHAR(50) NOT NULL,
    salary DECIMAL(10,2) CHECK (salary >= 0),
    hire_date DATE NOT NULL,
    manager_id INTEGER REFERENCES employees(employee_id),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create attendance table
CREATE TABLE attendance (
    attendance_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES employees(employee_id),
    attendance_date DATE NOT NULL,
    clock_in_time TIME,
    clock_out_time TIME,
    hours_worked DECIMAL(4,2) CHECK (hours_worked >= 0 AND hours_worked <= 24),
    status VARCHAR(20) DEFAULT 'present',
    notes TEXT,
    CONSTRAINT valid_attendance_status CHECK (status IN ('present', 'absent', 'sick', 'vacation', 'holiday'))
);

-- Create performance_reviews table
CREATE TABLE performance_reviews (
    review_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES employees(employee_id),
    reviewer_id INTEGER REFERENCES employees(employee_id),
    review_period VARCHAR(20) NOT NULL,
    overall_rating INTEGER CHECK (overall_rating BETWEEN 1 AND 5),
    goals_met BOOLEAN,
    review_notes TEXT,
    review_date DATE NOT NULL,
    next_review_date DATE
);

-- Create departments table
CREATE TABLE departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(50) UNIQUE NOT NULL,
    department_head INTEGER REFERENCES employees(employee_id),
    budget DECIMAL(12,2) DEFAULT 0.00,
    location VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create leave_requests table
CREATE TABLE leave_requests (
    request_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES employees(employee_id),
    leave_type VARCHAR(30) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    days_requested INTEGER NOT NULL CHECK (days_requested > 0),
    reason TEXT,
    status VARCHAR(20) DEFAULT 'pending',
    approved_by INTEGER REFERENCES employees(employee_id),
    request_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_leave_type CHECK (leave_type IN ('vacation', 'sick', 'personal', 'maternity', 'paternity')),
    CONSTRAINT valid_leave_status CHECK (status IN ('pending', 'approved', 'denied', 'cancelled'))
);

-- Insert sample employees (managers first)
INSERT INTO employees (employee_number, first_name, last_name, email, department, position, salary, hire_date, manager_id) VALUES
('EMP001', 'Alice', 'Wilson', 'alice.wilson@company.com', 'Engineering', 'Engineering Manager', 125000.00, '2022-01-15', NULL),
('EMP002', 'Carlos', 'Martinez', 'carlos.martinez@company.com', 'Marketing', 'Marketing Manager', 95000.00, '2022-03-01', NULL),
('EMP003', 'Sarah', 'Chen', 'sarah.chen@company.com', 'Engineering', 'Senior Developer', 105000.00, '2022-06-10', 1),
('EMP004', 'Michael', 'Brown', 'michael.brown@company.com', 'Sales', 'Sales Manager', 85000.00, '2022-09-01', NULL),
('EMP005', 'Jennifer', 'Davis', 'jennifer.davis@company.com', 'Engineering', 'DevOps Engineer', 98000.00, '2023-01-15', 1),
('EMP006', 'David', 'Johnson', 'david.johnson@company.com', 'Marketing', 'Marketing Specialist', 68000.00, '2023-03-20', 2),
('EMP007', 'Lisa', 'Anderson', 'lisa.anderson@company.com', 'Sales', 'Sales Representative', 65000.00, '2023-05-10', 4),
('EMP008', 'Robert', 'Taylor', 'robert.taylor@company.com', 'Engineering', 'Junior Developer', 75000.00, '2023-08-01', 1),
('EMP009', 'Emily', 'White', 'emily.white@company.com', 'HR', 'HR Manager', 85000.00, '2023-02-15', NULL),
('EMP010', 'James', 'Garcia', 'james.garcia@company.com', 'Finance', 'Finance Manager', 90000.00, '2023-04-01', NULL);

-- Insert departments
INSERT INTO departments (department_name, department_head, budget, location) VALUES
('Engineering', 1, 500000.00, 'Building A, Floor 3'),
('Marketing', 2, 200000.00, 'Building B, Floor 2'),
('Sales', 4, 300000.00, 'Building B, Floor 1'),
('HR', 9, 150000.00, 'Building A, Floor 1'),
('Finance', 10, 180000.00, 'Building A, Floor 2');

-- Insert sample attendance for the last week
INSERT INTO attendance (employee_id, attendance_date, clock_in_time, clock_out_time, hours_worked, status) VALUES
-- Monday 2025-09-08
(1, '2025-09-08', '08:30:00', '17:00:00', 8.5, 'present'),
(2, '2025-09-08', '09:00:00', '17:30:00', 8.5, 'present'),
(3, '2025-09-08', '09:15:00', '18:00:00', 8.75, 'present'),
(4, '2025-09-08', '08:45:00', '17:15:00', 8.5, 'present'),
(5, '2025-09-08', NULL, NULL, 0, 'sick'),
-- Tuesday 2025-09-09
(1, '2025-09-09', '08:30:00', '17:00:00', 8.5, 'present'),
(2, '2025-09-09', '09:00:00', '17:30:00', 8.5, 'present'),
(3, '2025-09-09', '09:00:00', '17:45:00', 8.75, 'present'),
(4, '2025-09-09', '08:30:00', '17:00:00', 8.5, 'present'),
(5, '2025-09-09', '09:30:00', '18:00:00', 8.5, 'present'),
-- Wednesday 2025-09-10
(1, '2025-09-10', '08:30:00', '17:00:00', 8.5, 'present'),
(2, '2025-09-10', NULL, NULL, 0, 'vacation'),
(3, '2025-09-10', '09:15:00', '18:00:00', 8.75, 'present'),
(4, '2025-09-10', '08:45:00', '17:15:00', 8.5, 'present'),
(5, '2025-09-10', '09:00:00', '17:30:00', 8.5, 'present');

-- Insert sample performance reviews
INSERT INTO performance_reviews (employee_id, reviewer_id, review_period, overall_rating, goals_met, review_notes, review_date, next_review_date) VALUES
(3, 1, 'Q2-2025', 4, true, 'Excellent technical skills, great team collaboration', '2025-07-15', '2025-10-15'),
(5, 1, 'Q2-2025', 5, true, 'Outstanding performance in DevOps initiatives', '2025-07-20', '2025-10-20'),
(6, 2, 'Q2-2025', 3, false, 'Good progress but needs to improve campaign metrics', '2025-07-25', '2025-10-25'),
(7, 4, 'Q2-2025', 4, true, 'Exceeded sales targets, excellent customer relations', '2025-07-30', '2025-10-30'),
(8, 1, 'Q2-2025', 3, true, 'Good progress for junior developer, on track', '2025-08-05', '2025-11-05');

-- Insert sample leave requests
INSERT INTO leave_requests (employee_id, leave_type, start_date, end_date, days_requested, reason, status, approved_by) VALUES
(2, 'vacation', '2025-09-10', '2025-09-12', 3, 'Family vacation', 'approved', 1),
(3, 'sick', '2025-09-05', '2025-09-05', 1, 'Medical appointment', 'approved', 1),
(6, 'personal', '2025-09-15', '2025-09-15', 1, 'Personal matters', 'pending', NULL),
(7, 'vacation', '2025-09-20', '2025-09-25', 4, 'Wedding anniversary trip', 'approved', 4),
(8, 'sick', '2025-09-08', '2025-09-09', 2, 'Flu symptoms', 'approved', 1);

-- Create indexes
CREATE INDEX idx_employees_department ON employees(department);
CREATE INDEX idx_employees_manager ON employees(manager_id);
CREATE INDEX idx_attendance_employee_date ON attendance(employee_id, attendance_date);
CREATE INDEX idx_attendance_date ON attendance(attendance_date);
CREATE INDEX idx_reviews_employee ON performance_reviews(employee_id);
CREATE INDEX idx_leave_requests_employee ON leave_requests(employee_id);

-- ===============================================
-- finance_db Setup
-- ===============================================
\c finance_db;

-- Create accounts table
CREATE TABLE accounts (
    account_id SERIAL PRIMARY KEY,
    account_number VARCHAR(20) UNIQUE NOT NULL,
    account_name VARCHAR(100) NOT NULL,
    account_type VARCHAR(30) NOT NULL,
    balance DECIMAL(15,2) DEFAULT 0.00,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_account_type CHECK (account_type IN ('asset', 'liability', 'equity', 'revenue', 'expense'))
);

-- Create transactions table
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    transaction_number VARCHAR(30) UNIQUE NOT NULL,
    transaction_date DATE NOT NULL,
    description TEXT NOT NULL,
    debit_account_id INTEGER REFERENCES accounts(account_id),
    credit_account_id INTEGER REFERENCES accounts(account_id),
    amount DECIMAL(12,2) NOT NULL CHECK (amount > 0),
    reference_number VARCHAR(50),
    created_by VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create budget_allocations table
CREATE TABLE budget_allocations (
    allocation_id SERIAL PRIMARY KEY,
    department VARCHAR(50) NOT NULL,
    budget_category VARCHAR(50) NOT NULL,
    fiscal_year INTEGER NOT NULL,
    allocated_amount DECIMAL(12,2) NOT NULL CHECK (allocated_amount >= 0),
    spent_amount DECIMAL(12,2) DEFAULT 0.00 CHECK (spent_amount >= 0),
    remaining_amount DECIMAL(12,2) GENERATED ALWAYS AS (allocated_amount - spent_amount) STORED,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create invoices table
CREATE TABLE invoices (
    invoice_id SERIAL PRIMARY KEY,
    invoice_number VARCHAR(30) UNIQUE NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    invoice_date DATE NOT NULL,
    due_date DATE NOT NULL,
    subtotal DECIMAL(10,2) NOT NULL CHECK (subtotal >= 0),
    tax_amount DECIMAL(8,2) DEFAULT 0.00 CHECK (tax_amount >= 0),
    total_amount DECIMAL(10,2) NOT NULL CHECK (total_amount >= 0),
    status VARCHAR(20) DEFAULT 'pending',
    paid_date DATE,
    CONSTRAINT valid_invoice_status CHECK (status IN ('pending', 'sent', 'paid', 'overdue', 'cancelled'))
);

-- Create expenses table
CREATE TABLE expenses (
    expense_id SERIAL PRIMARY KEY,
    expense_number VARCHAR(30) UNIQUE NOT NULL,
    department VARCHAR(50) NOT NULL,
    category VARCHAR(50) NOT NULL,
    description TEXT NOT NULL,
    amount DECIMAL(10,2) NOT NULL CHECK (amount > 0),
    expense_date DATE NOT NULL,
    submitted_by VARCHAR(50) NOT NULL,
    approved_by VARCHAR(50),
    status VARCHAR(20) DEFAULT 'pending',
    receipt_url VARCHAR(200),
    CONSTRAINT valid_expense_status CHECK (status IN ('pending', 'approved', 'rejected', 'reimbursed'))
);

-- Insert sample accounts
INSERT INTO accounts (account_number, account_name, account_type, balance) VALUES
('1000', 'Cash - Operating Account', 'asset', 325000.00),
('1100', 'Accounts Receivable', 'asset', 85000.00),
('1200', 'Inventory', 'asset', 150000.00),
('1500', 'Equipment', 'asset', 75000.00),
('2000', 'Accounts Payable', 'liability', 45000.00),
('2100', 'Accrued Expenses', 'liability', 15000.00),
('3000', 'Owner Equity', 'equity', 500000.00),
('4000', 'Sales Revenue', 'revenue', 0.00),
('4100', 'Service Revenue', 'revenue', 0.00),
('5000', 'Office Expenses', 'expense', 0.00),
('5100', 'Marketing Expenses', 'expense', 0.00),
('5200', 'Payroll Expenses', 'expense', 0.00);

-- Insert sample transactions
INSERT INTO transactions (transaction_number, transaction_date, description, debit_account_id, credit_account_id, amount, reference_number, created_by) VALUES
('TXN-2025-001', '2025-09-01', 'Sales revenue for August', 1, 8, 25000.00, 'INV-2025-001', 'system'),
('TXN-2025-002', '2025-09-02', 'Office supplies purchase', 10, 1, 1250.00, 'EXP-2025-001', 'accounting'),
('TXN-2025-003', '2025-09-03', 'Marketing campaign costs', 11, 1, 3500.00, 'EXP-2025-002', 'accounting'),
('TXN-2025-004', '2025-09-05', 'Payroll for August', 12, 1, 45000.00, 'PAY-2025-001', 'hr'),
('TXN-2025-005', '2025-09-08', 'Customer payment received', 1, 2, 12000.00, 'PAY-2025-002', 'accounting'),
('TXN-2025-006', '2025-09-10', 'Equipment purchase', 4, 1, 8500.00, 'EXP-2025-003', 'accounting');

-- Insert sample budget allocations
INSERT INTO budget_allocations (department, budget_category, fiscal_year, allocated_amount, spent_amount) VALUES
('Engineering', 'Software Licenses', 2025, 50000.00, 18500.00),
('Engineering', 'Hardware', 2025, 75000.00, 32000.00),
('Marketing', 'Advertising', 2025, 80000.00, 28000.00),
('Marketing', 'Events', 2025, 35000.00, 12500.00),
('HR', 'Training & Development', 2025, 30000.00, 8500.00),
('HR', 'Recruitment', 2025, 25000.00, 6000.00),
('Sales', 'Travel', 2025, 40000.00, 15000.00),
('Finance', 'Professional Services', 2025, 20000.00, 5500.00);

-- Insert sample invoices
INSERT INTO invoices (invoice_number, customer_name, invoice_date, due_date, subtotal, tax_amount, total_amount, status, paid_date) VALUES
('INV-2025-001', 'TechCorp Solutions', '2025-08-30', '2025-09-29', 12000.00, 960.00, 12960.00, 'paid', '2025-09-08'),
('INV-2025-002', 'Global Industries', '2025-09-01', '2025-10-01', 8500.00, 680.00, 9180.00, 'sent', NULL),
('INV-2025-003', 'StartupXYZ', '2025-09-05', '2025-10-05', 5200.00, 416.00, 5616.00, 'pending', NULL),
('INV-2025-004', 'Enterprise LLC', '2025-09-08', '2025-10-08', 15000.00, 1200.00, 16200.00, 'sent', NULL),
('INV-2025-005', 'MegaCorp Inc', '2025-09-10', '2025-10-10', 22000.00, 1760.00, 23760.00, 'pending', NULL);

-- Insert sample expenses
INSERT INTO expenses (expense_number, department, category, description, amount, expense_date, submitted_by, approved_by, status) VALUES
('EXP-2025-001', 'Engineering', 'Software', 'Annual IDE licenses', 2500.00, '2025-09-01', 'alice.wilson', 'management', 'approved'),
('EXP-2025-002', 'Marketing', 'Advertising', 'Google Ads campaign', 1800.00, '2025-09-02', 'carlos.martinez', 'management', 'approved'),
('EXP-2025-003', 'Sales', 'Travel', 'Client meeting travel', 650.00, '2025-09-03', 'michael.brown', 'management', 'reimbursed'),
('EXP-2025-004', 'HR', 'Training', 'Conference registration', 1200.00, '2025-09-05', 'emily.white', NULL, 'pending'),
('EXP-2025-005', 'Engineering', 'Hardware', 'Development server', 3500.00, '2025-09-08', 'sarah.chen', 'management', 'approved');

-- Create indexes
CREATE INDEX idx_accounts_type ON accounts(account_type);
CREATE INDEX idx_transactions_date ON transactions(transaction_date);
CREATE INDEX idx_transactions_debit ON transactions(debit_account_id);
CREATE INDEX idx_transactions_credit ON transactions(credit_account_id);
CREATE INDEX idx_budget_department ON budget_allocations(department);
CREATE INDEX idx_invoices_status ON invoices(status);
CREATE INDEX idx_expenses_department ON expenses(department);

-- ===============================================
-- reporting_db Setup
-- ===============================================
\c reporting_db;

-- Create daily_reports table
CREATE TABLE daily_reports (
    report_id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL UNIQUE,
    total_sales DECIMAL(12,2) DEFAULT 0.00,
    new_customers INTEGER DEFAULT 0,
    active_employees INTEGER DEFAULT 0,
    cash_balance DECIMAL(15,2) DEFAULT 0.00,
    inventory_value DECIMAL(12,2) DEFAULT 0.00,
    orders_count INTEGER DEFAULT 0,
    avg_order_value DECIMAL(8,2) DEFAULT 0.00,
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create kpi_tracking table
CREATE TABLE kpi_tracking (
    kpi_id SERIAL PRIMARY KEY,
    kpi_name VARCHAR(100) NOT NULL,
    kpi_value DECIMAL(12,4) NOT NULL,
    target_value DECIMAL(12,4),
    measurement_period VARCHAR(20),
    measurement_date DATE NOT NULL,
    department VARCHAR(50),
    notes TEXT,
    CONSTRAINT valid_period CHECK (measurement_period IN ('daily', 'weekly', 'monthly', 'quarterly', 'yearly'))
);

-- Create executive_dashboard table
CREATE TABLE executive_dashboard (
    dashboard_id SERIAL PRIMARY KEY,
    metric_category VARCHAR(50) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    current_value DECIMAL(15,4),
    previous_value DECIMAL(15,4),
    change_percentage DECIMAL(5,2),
    trend VARCHAR(20), -- 'up', 'down', 'stable'
    update_date DATE NOT NULL,
    data_source VARCHAR(50)
);

-- Insert sample daily reports
INSERT INTO daily_reports (report_date, total_sales, new_customers, active_employees, cash_balance, inventory_value, orders_count, avg_order_value) VALUES
('2025-09-01', 2450.75, 2, 10, 325000.00, 148500.00, 12, 204.23),
('2025-09-02', 1890.50, 1, 10, 323750.00, 147250.00, 9, 210.06),
('2025-09-03', 3200.25, 3, 10, 326950.25, 149750.25, 15, 213.35),
('2025-09-04', 1650.00, 0, 10, 325300.25, 148100.25, 8, 206.25),
('2025-09-05', 2890.75, 2, 10, 328191.00, 150641.00, 14, 206.48),
('2025-09-06', 2150.50, 1, 10, 330341.50, 149791.50, 11, 195.50),
('2025-09-07', 1750.25, 1, 10, 332091.75, 148541.75, 7, 250.04),
('2025-09-08', 2950.00, 2, 10, 335041.75, 151491.75, 13, 226.92),
('2025-09-09', 2100.75, 1, 10, 337142.50, 150342.50, 10, 210.08),
('2025-09-10', 3450.50, 3, 10, 340593.00, 153793.00, 16, 215.66);

-- Insert sample KPI tracking
INSERT INTO kpi_tracking (kpi_name, kpi_value, target_value, measurement_period, measurement_date, department, notes) VALUES
('Customer Acquisition Cost', 125.50, 100.00, 'monthly', '2025-09-01', 'Marketing', 'Slightly above target'),
('Employee Satisfaction Score', 4.2, 4.0, 'quarterly', '2025-09-01', 'HR', 'Above target, good progress'),
('Revenue Growth Rate', 12.5, 15.0, 'monthly', '2025-09-01', 'Sales', 'Below target, needs attention'),
('Inventory Turnover', 8.2, 10.0, 'monthly', '2025-09-01', 'Operations', 'Below target'),
('Gross Profit Margin', 45.8, 50.0, 'monthly', '2025-09-01', 'Finance', 'Need to optimize costs'),
('Customer Retention Rate', 87.5, 85.0, 'monthly', '2025-09-01', 'Sales', 'Above target'),
('Average Order Value', 210.50, 200.00, 'weekly', '2025-09-08', 'Sales', 'Meeting target'),
('Code Deployment Frequency', 15.0, 12.0, 'weekly', '2025-09-08', 'Engineering', 'Above target'),
('System Uptime', 99.8, 99.5, 'monthly', '2025-09-01', 'Engineering', 'Excellent performance'),
('Budget Variance', 5.2, 5.0, 'monthly', '2025-09-01', 'Finance', 'Within acceptable range');

-- Insert sample executive dashboard metrics
INSERT INTO executive_dashboard (metric_category, metric_name, current_value, previous_value, change_percentage, trend, update_date, data_source) VALUES
('Financial', 'Monthly Revenue', 235000.00, 220000.00, 6.82, 'up', '2025-09-10', 'finance_db'),
('Financial', 'Cash Position', 340593.00, 325000.00, 4.80, 'up', '2025-09-10', 'finance_db'),
('Financial', 'Gross Margin %', 45.8, 43.2, 6.02, 'up', '2025-09-10', 'analytics_db'),
('Operations', 'Inventory Value', 153793.00, 148500.00, 3.57, 'up', '2025-09-10', 'inventory_db'),
('Operations', 'Order Fulfillment Rate', 98.5, 97.2, 1.34, 'up', '2025-09-10', 'customer_db'),
('Sales', 'New Customers', 16.0, 12.0, 33.33, 'up', '2025-09-10', 'customer_db'),
('Sales', 'Average Order Value', 215.66, 204.23, 5.60, 'up', '2025-09-10', 'customer_db'),
('HR', 'Active Employees', 10.0, 10.0, 0.00, 'stable', '2025-09-10', 'hr_db'),
('HR', 'Employee Utilization', 92.5, 89.0, 3.93, 'up', '2025-09-10', 'hr_db'),
('Engineering', 'System Uptime %', 99.8, 99.2, 0.60, 'up', '2025-09-10', 'monitoring');

-- Create indexes
CREATE INDEX idx_daily_reports_date ON daily_reports(report_date);
CREATE INDEX idx_kpi_tracking_date ON kpi_tracking(measurement_date);
CREATE INDEX idx_kpi_tracking_department ON kpi_tracking(department);
CREATE INDEX idx_executive_dashboard_category ON executive_dashboard(metric_category);
CREATE INDEX idx_executive_dashboard_date ON executive_dashboard(update_date);
EOF

echo "SQL setup files created"

# Step 3: Run Database Setup
echo -e "${CYAN}=== Step 3: Setting Up Databases ===${NC}"

# Setup Business Operations (PG1) databases
if run_sql_file "sql/setup_pg1.sql" $PG_PORT "Business Operations Databases (PG1)"; then
    echo -e "${GREEN}Business Operations Databases created:${NC}"
    echo "  📊 customer_db: customers, orders, order_items (3 tables)"
    echo "  📦 inventory_db: products, inventory, inventory_movements (3 tables)"
    echo "  📈 analytics_db: sales_metrics, customer_analytics, product_analytics (3 tables)"
    echo ""
fi

# Setup HR & Finance (PG2) databases
if run_sql_file "sql/setup_pg2.sql" $PG_PORT "HR & Finance Databases (PG2)"; then
    echo -e "${GREEN}HR & Finance Databases created:${NC}"
    echo "  👥 hr_db: employees, attendance, performance_reviews, departments, leave_requests (5 tables)"
    echo "  💰 finance_db: accounts, transactions, budget_allocations, invoices, expenses (5 tables)"
    echo "  📋 reporting_db: daily_reports, kpi_tracking, executive_dashboard (3 tables)"
    echo ""
fi

# Step 4: Create Environment Configuration
echo -e "${CYAN}=== Step 4: Creating Environment Configuration ===${NC}"

cat > .env << EOF
# MCP servers (Update these URLs as needed)
MCP1_BASE_URL=http://localhost:8001
MCP1_API_KEY=

MCP2_BASE_URL=http://localhost:8002
MCP2_API_KEY=

# Ollama
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=llama3.1

# Optional LLM params
OLLAMA_TEMPERATURE=0.2
OLLAMA_NUM_CTX=8192

# PostgreSQL
POSTGRES_USER=$POSTGRES_USER
POSTGRES_PORT=$PG_PORT
EOF

echo "Environment configuration created (.env)"

# Step 5: Create Requirements File
echo -e "${CYAN}=== Step 5: Creating Python Requirements ===${NC}"

cat > requirements.txt << 'EOF'
# Core dependencies
python-dotenv==1.0.0
typer[all]==0.9.0
rich==13.6.0

# LangChain ecosystem
langchain==0.0.339
langchain-community==0.0.38
langchain-experimental==0.0.45
langgraph==0.0.66

# HTTP and async
httpx==0.25.2
uvicorn[standard]==0.24.0
fastapi==0.104.1

# Data validation
pydantic==2.5.0

# PostgreSQL
psycopg2-binary==2.9.7

# Utilities
tqdm==4.66.1
jsondiff==2.0.0
EOF

echo "Python requirements created (requirements.txt)"

# Step 6: Create setup summary
echo -e "${CYAN}=== Step 6: Installation Summary ===${NC}"

cat > DEPLOYMENT_SUMMARY.md << EOF
# PostgreSQL Backup System - Deployment Summary

## System Configuration
- **PostgreSQL Server**: localhost:$PG_PORT
- **PostgreSQL User**: $POSTGRES_USER
- **System Name**: $SYSTEM_NAME
- **Deployment Date**: $(date)

## Databases Created

### Business Operations (PG1)
1. **customer_db** - Customer management and CRM data
   - customers (8 sample records)
   - orders (8 sample records)
   - order_items (10 sample records)

2. **inventory_db** - Product inventory and warehouse management
   - products (8 sample records)
   - inventory (8 sample records)
   - inventory_movements (8 sample records)

3. **analytics_db** - Business intelligence and reporting data
   - sales_metrics (10 sample records)
   - customer_analytics (8 sample records)
   - product_analytics (8 sample records)

### HR & Finance (PG2)
1. **hr_db** - Human resources management
   - employees (10 sample records)
   - departments (5 sample records)
   - attendance (15 sample records)
   - performance_reviews (5 sample records)
   - leave_requests (5 sample records)

2. **finance_db** - Financial transactions and accounting
   - accounts (12 sample records)
   - transactions (6 sample records)
   - budget_allocations (8 sample records)
   - invoices (5 sample records)
   - expenses (5 sample records)

3. **reporting_db** - Cross-system reporting and data warehousing
   - daily_reports (10 sample records)
   - kpi_tracking (10 sample records)
   - executive_dashboard (10 sample records)

## Total Database Summary
- **6 databases** across logical server groups
- **21 tables** with comprehensive business data
- **165+ sample records** for testing and demonstration

## Next Steps

### 1. Install Python Dependencies
\`\`\`bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\\Scripts\\activate

# Install dependencies
pip install -r requirements.txt
\`\`\`

### 2. Install and Setup Ollama (for LLM functionality)
\`\`\`bash
# Install Ollama (macOS)
brew install ollama

# Start Ollama service
ollama serve

# Pull the required model
ollama pull llama3.1
\`\`\`

### 3. Copy Backup System Code
Copy the following files from the original system:
- \`true_wal_incremental_backup.py\` - Main backup server
- \`cli.py\` - Command line interface
- \`graph.py\` - Orchestration logic
- \`mcp/\` directory - MCP client and tools
- \`agents/\` directory - Backup agents
- \`llm/\` directory - LLM helpers
- \`supervisor/\` directory - Supervisor logic

### 4. Test Database Connectivity
\`\`\`bash
# Test PostgreSQL connection
psql -h localhost -p $PG_PORT -U $POSTGRES_USER -l

# Test sample queries
psql -h localhost -p $PG_PORT -U $POSTGRES_USER -d customer_db -c "SELECT COUNT(*) FROM customers;"
psql -h localhost -p $PG_PORT -U $POSTGRES_USER -d hr_db -c "SELECT COUNT(*) FROM employees;"
\`\`\`

### 5. Start the Backup System
\`\`\`bash
# Start MCP servers (after copying code)
python true_wal_incremental_backup.py

# In another terminal, start the CLI
python -m cli run
\`\`\`

## Files Created
- \`sql/setup_pg1.sql\` - Business operations database setup
- \`sql/setup_pg2.sql\` - HR & finance database setup  
- \`.env\` - Environment configuration
- \`requirements.txt\` - Python dependencies
- \`DEPLOYMENT_SUMMARY.md\` - This summary file

## Database Connection Examples
\`\`\`bash
# Connect to customer database
psql -h localhost -p $PG_PORT -U $POSTGRES_USER -d customer_db

# Connect to HR database
psql -h localhost -p $PG_PORT -U $POSTGRES_USER -d hr_db

# List all databases
psql -h localhost -p $PG_PORT -U $POSTGRES_USER -l
\`\`\`

## Sample Queries to Test
\`\`\`sql
-- Customer data
SELECT c.first_name, c.last_name, COUNT(o.order_id) as order_count 
FROM customer_db.customers c 
LEFT JOIN customer_db.orders o ON c.customer_id = o.customer_id 
GROUP BY c.customer_id, c.first_name, c.last_name;

-- Employee data  
SELECT e.first_name, e.last_name, e.department, e.position 
FROM hr_db.employees e 
WHERE e.is_active = true;

-- Financial summary
SELECT account_type, SUM(balance) as total_balance 
FROM finance_db.accounts 
GROUP BY account_type;
\`\`\`

EOF

echo -e "Databases created"
