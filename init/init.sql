CREATE TABLE IF NOT EXISTS dim_product_status(
    status_id CHAR PRIMARY KEY,
    label VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_category_level_1(
    category_level_1_id INT PRIMARY KEY,
    category_level_1_label VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_category_level_2(
    category_level_2_id INT PRIMARY KEY,
    category_level_2_label VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_category_level_3(
    category_level_3_id INT PRIMARY KEY,
    category_level_3_label VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_product(
    sku_bc VARCHAR(255) PRIMARY KEY,                                                -- Sku BC code
    sku_cap VARCHAR(255),                                                           -- Sku Cap code
    article_label VARCHAR(255),                                                     -- Article label
    status_id CHAR REFERENCES dim_product_status(status_id),                             -- Article Status ID  
    category_level_1_id INT REFERENCES dim_category_level_1(category_level_1_id),   -- ID of cat level 1
    category_level_2_id INT REFERENCES dim_category_level_2(category_level_2_id),   -- ID of cat level 2
    category_level_3_id INT REFERENCES dim_category_level_3(category_level_3_id)   -- ID of cat level 3
);

CREATE TABLE IF NOT EXISTS dim_date(
    date_ DATE PRIMARY KEY,
    day_of_month INT,
    day_of_year INT,
    weekday_name VARCHAR(50),
    week_ INT,
    month_ INT,
    month_name VARCHAR(50),
    year_ INT,
    semester INT,
    quarter_ INT
);

CREATE TABLE IF NOT EXISTS dim_geographical (
    geographical_id SERIAL PRIMARY KEY,
    region VARCHAR(255),  
    departement VARCHAR(255),
    city VARCHAR(255),
    zip_code VARCHAR(10)
);
INSERT INTO dim_geographical(geographical_id, region, departement, city, zip_code) VALUES
(2, 'Île-de-France', 'Paris', 'Paris', 75015);

CREATE TABLE IF NOT EXISTS dim_shop(
    shop_id INT PRIMARY KEY,
    shop_name VARCHAR(255),
    geographical_id INT REFERENCES dim_geographical(geographical_id) 
);
INSERT INTO dim_shop(shop_id, shop_name, geographical_id) VALUES
(100, 'Shop Paris 15', 2);

CREATE TABLE IF NOT EXISTS fact_stock (
    stock_id SERIAL PRIMARY KEY,                     
    sku_bc VARCHAR(255) REFERENCES dim_product(sku_bc),
    shop_id INT REFERENCES dim_shop(shop_id),
    date_ DATE REFERENCES dim_date(date_),
    stock_quantity INT,                           
    valuation DECIMAL(15, 2)                      
);