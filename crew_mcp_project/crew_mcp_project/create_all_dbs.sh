#!/bin/bash

# --- PostgreSQL superuser ---
PGUSER="postgres"
export PGPASSWORD=""

# --- PostgreSQL cluster data directories ---
PG1_DATA="/var/lib/postgresql/17/pg1_17"
PG2_DATA="/var/lib/postgresql/17/pg2_17"

# --- Helper function to execute queries ---
exec_query() {
    local DB=$1
    local PORT=$2
    local QUERY=$3
    psql -h 127.0.0.1 -p $PORT -U $PGUSER -d $DB -c "$QUERY"
}

# --- Helper function to create database if it doesn't exist ---
create_db() {
    local DBNAME=$1
    local PORT=$2
    local EXISTS=$(psql -h 127.0.0.1 -p $PORT -U $PGUSER -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$DBNAME';" 2>/dev/null)
    if [[ -z "$EXISTS" ]]; then
        exec_query "postgres" $PORT "CREATE DATABASE $DBNAME;"
        echo "Database $DBNAME created on port $PORT."
    else
        echo "Database $DBNAME already exists on port $PORT. Using existing DB."
    fi
}

# --- Helper function to check if a port is open ---
is_port_open() {
    local PORT=$1
    nc -z 127.0.0.1 $PORT &>/dev/null
    return $?
}

# --- Start PostgreSQL clusters if not running ---
start_cluster() {
    local DATA_DIR=$1
    local PORT=$2
    if ! is_port_open $PORT; then
        echo "Starting PostgreSQL cluster on port $PORT..."
        sudo pg_ctlcluster 17 $(basename $DATA_DIR) start
        sleep 3
    else
        echo "PostgreSQL cluster already running on port $PORT."
    fi
}

# --- Start clusters ---
start_cluster $PG1_DATA 5433
start_cluster $PG2_DATA 5434

# --- Create databases ---
create_db "db1" 5433
create_db "db2" 5433
create_db "db3" 5434
create_db "db4" 5434

# --- Drop and recreate all tables (clean reset each run) ---

# db1
exec_query "db1" 5433 "
DROP TABLE IF EXISTS users, customers, orders, products, transactions CASCADE;
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    email VARCHAR(50) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(100),
    email VARCHAR(50) UNIQUE,
    joined_on DATE DEFAULT CURRENT_DATE
);
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INT,
    order_date DATE DEFAULT CURRENT_DATE,
    amount DECIMAL(10,2)
);
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2)
);
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    order_id INT,
    payment_method VARCHAR(50),
    status VARCHAR(50)
);
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com'), ('Bob', 'bob@example.com');
INSERT INTO customers (full_name, email) VALUES ('Charlie Brown', 'charlie@example.com'), ('Diana Prince', 'diana@example.com');
INSERT INTO orders (customer_id, amount) VALUES (1, 250.75), (2, 420.50);
INSERT INTO products (name, price) VALUES ('Laptop', 899.99), ('Phone', 499.99);
INSERT INTO transactions (order_id, payment_method, status) VALUES (1, 'Credit Card', 'Completed'), (2, 'PayPal', 'Pending');
"

# db2
exec_query "db2" 5433 "
DROP TABLE IF EXISTS customers, orders, products, transactions, reviews CASCADE;
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(100),
    email VARCHAR(50) UNIQUE,
    joined_on DATE DEFAULT CURRENT_DATE
);
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INT,
    order_date DATE DEFAULT CURRENT_DATE,
    total DECIMAL(10,2)
);
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10,2)
);
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    order_id INT,
    method VARCHAR(50),
    status VARCHAR(50)
);
CREATE TABLE reviews (
    id SERIAL PRIMARY KEY,
    product_id INT,
    rating INT,
    comment TEXT
);
INSERT INTO customers (full_name, email) VALUES ('Ethan Hunt', 'ethan@example.com'), ('Grace Lee', 'grace@example.com');
INSERT INTO orders (customer_id, total) VALUES (1, 150.00), (2, 320.00);
INSERT INTO products (name, category, price) VALUES ('Tablet', 'Electronics', 299.99), ('Headphones', 'Audio', 79.99);
INSERT INTO transactions (order_id, method, status) VALUES (1, 'UPI', 'Completed'), (2, 'Credit Card', 'Processing');
INSERT INTO reviews (product_id, rating, comment) VALUES (1, 5, 'Excellent product!'), (2, 4, 'Good quality.');
"

# db3
exec_query "db3" 5434 "
DROP TABLE IF EXISTS employees, projects, salaries, attendance, departments CASCADE;
CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    department VARCHAR(50),
    hired_on DATE DEFAULT CURRENT_DATE
);
CREATE TABLE projects (
    id SERIAL PRIMARY KEY,
    project_name VARCHAR(100),
    start_date DATE DEFAULT CURRENT_DATE,
    end_date DATE
);
CREATE TABLE salaries (
    id SERIAL PRIMARY KEY,
    employee_id INT,
    amount DECIMAL(10,2),
    pay_date DATE DEFAULT CURRENT_DATE
);
CREATE TABLE attendance (
    id SERIAL PRIMARY KEY,
    employee_id INT,
    present BOOLEAN DEFAULT TRUE,
    date DATE DEFAULT CURRENT_DATE
);
CREATE TABLE departments (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    location VARCHAR(50)
);
INSERT INTO employees (name, department) VALUES ('Eve Adams', 'HR'), ('Frank Miller', 'IT');
INSERT INTO projects (project_name, start_date, end_date) VALUES ('Website Redesign', '2025-09-01', '2025-12-01'), ('Mobile App', '2025-08-15', '2025-11-15');
INSERT INTO salaries (employee_id, amount) VALUES (1, 5000.00), (2, 6500.00);
INSERT INTO attendance (employee_id, present) VALUES (1, TRUE), (2, TRUE);
INSERT INTO departments (name, location) VALUES ('HR', 'New York'), ('IT', 'San Francisco');
"

# db4
exec_query "db4" 5434 "
DROP TABLE IF EXISTS employees, projects, salaries, attendance, departments CASCADE;
CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    department VARCHAR(50),
    hired_on DATE DEFAULT CURRENT_DATE
);
CREATE TABLE projects (
    id SERIAL PRIMARY KEY,
    project_name VARCHAR(100),
    start_date DATE DEFAULT CURRENT_DATE,
    end_date DATE
);
CREATE TABLE salaries (
    id SERIAL PRIMARY KEY,
    employee_id INT,
    amount DECIMAL(10,2),
    pay_date DATE DEFAULT CURRENT_DATE
);
CREATE TABLE attendance (
    id SERIAL PRIMARY KEY,
    employee_id INT,
    present BOOLEAN DEFAULT TRUE,
    date DATE DEFAULT CURRENT_DATE
);
CREATE TABLE departments (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    location VARCHAR(50)
);
INSERT INTO employees (name, department) VALUES ('John Doe', 'Finance'), ('Jane Smith', 'Operations');
INSERT INTO projects (project_name, start_date, end_date) VALUES ('ERP System', '2025-07-01', '2025-10-01'), ('Cloud Migration', '2025-08-01', '2025-11-30');
INSERT INTO salaries (employee_id, amount) VALUES (1, 7000.00), (2, 7500.00);
INSERT INTO attendance (employee_id, present) VALUES (1, TRUE), (2, FALSE);
INSERT INTO departments (name, location) VALUES ('Finance', 'Chicago'), ('Operations', 'Dallas');
"

echo "✅ All databases, tables, and sample data have been dropped and recreated successfully!"
