DROP TABLE IF EXISTS mock_data;

CREATE TABLE mock_data (
    id int,
    customer_first_name text,
    customer_last_name text,
    customer_age int,
    customer_email text,
    customer_country text,
    customer_postal_code text,
    customer_pet_type text,
    customer_pet_name text,
    customer_pet_breed text,
    seller_first_name text,
    seller_last_name text,
    seller_email text,
    seller_country text,
    seller_postal_code text,
    product_name text,
    product_category text,
    product_price numeric,
    product_quantity int,
    sale_date date,
    sale_customer_id int,
    sale_seller_id int,
    sale_product_id int,
    sale_quantity int,
    sale_total_price numeric,
    store_name text,
    store_location text,
    store_city text,
    store_state text,
    store_country text,
    store_phone text,
    store_email text,
    pet_category text,
    product_weight numeric,
    product_color text,
    product_size text,
    product_brand text,
    product_material text,
    product_description text,
    product_rating numeric,
    product_reviews int,
    product_release_date date,
    product_expiry_date date,
    supplier_name text,
    supplier_contact text,
    supplier_email text,
    supplier_phone text,
    supplier_address text,
    supplier_city text,
    supplier_country text
);

COPY mock_data FROM '/data/MOCK_DATA.csv' WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/data/MOCK_DATA (1).csv' WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/data/MOCK_DATA (2).csv' WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/data/MOCK_DATA (3).csv' WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/data/MOCK_DATA (4).csv' WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/data/MOCK_DATA (5).csv' WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/data/MOCK_DATA (6).csv' WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/data/MOCK_DATA (7).csv' WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/data/MOCK_DATA (8).csv' WITH (FORMAT csv, HEADER true);
COPY mock_data FROM '/data/MOCK_DATA (9).csv' WITH (FORMAT csv, HEADER true);
