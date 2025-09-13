-- ===============================================
-- PostgreSQL PG2 Setup Script  
-- Server: localhost:5433 (Secondary PostgreSQL instance)
-- MCP Server: http://localhost:8002
-- Databases: hr_db, finance_db, reporting_db
-- ===============================================

-- Connect as superuser to create databases
-- Run: psql -U postgres -h localhost -p 5433

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

-- Grant permissions (adjust as needed for your environment)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO backup_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO backup_user;

ECHO 'PG2 databases setup complete!';
ECHO 'Databases created: hr_db, finance_db, reporting_db';
ECHO 'Total tables: 12';
ECHO 'Sample data inserted for testing backup/restore operations';
