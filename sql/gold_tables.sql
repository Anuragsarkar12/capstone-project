
USE DATABASE CAPSTONE_DB;

USE SCHEMA PUBLIC;

USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE TABLE dim_customer (
surrogate_key INTEGER NOT NULL,
customer_id INTEGER NOT NULL,
full_name VARCHAR(200) NOT NULL,
email VARCHAR(200) NOT NULL,
country VARCHAR(100),
signup_date DATE,
CONSTRAINT pk_dim_customer PRIMARY KEY (surrogate_key)
);




CREATE OR REPLACE TABLE dim_product (
surrogate_key INTEGER NOT NULL,
product_id INTEGER NOT NULL,
product_name VARCHAR(300) NOT NULL,
category VARCHAR(100) NOT NULL,
CONSTRAINT pk_dim_product PRIMARY KEY (surrogate_key)
);

ALTER TABLE dim_product CLUSTER BY (category);

CREATE OR REPLACE TABLE fact_sales (
sale_id BIGINT NOT NULL,
order_id INTEGER NOT NULL,
order_item_id INTEGER NOT NULL,
customer_sk INTEGER NOT NULL,
product_sk INTEGER NOT NULL,
quantity INTEGER NOT NULL,
unit_price FLOAT NOT NULL,
total_amount FLOAT NOT NULL,
order_date DATE NOT NULL,
status VARCHAR(20) NOT NULL,
order_year INTEGER,
order_month INTEGER,
CONSTRAINT pk_fact_sales PRIMARY KEY (sale_id),
CONSTRAINT fk_fact_customer FOREIGN KEY (customer_sk) REFERENCES dim_customer(surrogate_key), 
CONSTRAINT fk_fact_product FOREIGN KEY (product_sk) REFERENCES dim_product(surrogate_key)
);




SELECT order_year, order_month,
COUNT(DISTINCT order_id) AS total_orders,
ROUND(SUM(total_amount), 2) AS total_revenue
FROM FACT_SALES
GROUP BY order_year, order_month
ORDER BY order_year, order_month;


