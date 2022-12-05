import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ECOMMERCE_DATA = config.get('S3', 'ECOMMERCE_DATA')
MARKETING_FUNNEL_DATA = config.get('S3', 'MARKETING_FUNNEL_DATA')
DWH_IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_customers_table_drop        = "DROP TABLE IF EXISTS staging_customers"
staging_geolocation_table_drop      = "DROP TABLE IF EXISTS staging_geolocation"
staging_items_table_drop            = "DROP TABLE IF EXISTS staging_items" 
staging_payments_table_drop         = "DROP TABLE IF EXISTS staging_payments"
staging_reviews_table_drop          = "DROP TABLE IF EXISTS staging_reviews"
staging_orders_table_drop           = "DROP TABLE IF EXISTS staging_orders"
staging_products_table_drop         = "DROP TABLE IF EXISTS staging_products"
staging_sellers_table_drop          = "DROP TABLE IF EXISTS staging_sellers"
staging_closed_deals_table_drop     = "DROP TABLE IF EXISTS staging_closed_deals"
staging_qualified_leads_table_drop  = "DROP TABLE IF EXISTS staging_qualified_leads"

fact_orders_table_drop    = "DROP TABLE IF EXISTS fact_orders"
dim_date_table_drop       = "DROP TABLE IF EXISTS dim_date"
dim_products_table_drop   = "DROP TABLE IF EXISTS dim_products"
dim_sellers_table_drop    = "DROP TABLE IF EXISTS dim_sellers"
dim_payments_table_drop   = "DROP TABLE IF EXISTS dim_payments"
dim_customers_table_drop  = "DROP TABLE IF EXISTS dim_customers"


# CREATE TABLES

staging_customers_table_create= (
        """
        CREATE TABLE staging_customers 
        (
            customer_id                 VARCHAR    NOT NULL,
            customer_unique_id          VARCHAR    NOT NULL,
            customer_zip_code_prefix    INT        NOT NULL,
            customer_city               VARCHAR    NOT NULL,
            customer_state              VARCHAR    NOT NULL
        );
        """
)

staging_geolocation_table_create= (
        """
        CREATE TABLE staging_geolocation 
        (
            geolocation_zip_code_prefix    INT        NOT NULL,
            geolocation_lat                FLOAT      NOT NULL, 
            geolocation_lng                FLOAT      NOT NULL,
            geolocation_city               VARCHAR    NOT NULL,
            geolocation_state              VARCHAR    NOT NULL
        );
        """
)

staging_items_table_create= (
        """
        CREATE TABLE staging_items 
        (
            order_id               VARCHAR      NOT NULL,
            order_item_id          INT          NOT NULL,
            product_id             VARCHAR      NOT NULL,
            seller_id              VARCHAR      NOT NULL,
            shipping_limit_date    TIMESTAMP    NOT NULL,
            price                  FLOAT        NOT NULL,
            freight_value          FLOAT        NOT NULL
        );
        """
)

staging_payments_table_create= (
        """
        CREATE TABLE staging_payments 
        (
            order_id                VARCHAR    NOT NULL,
            payment_sequential      INT        NOT NULL,
            payment_type            VARCHAR    NOT NULL,
            payment_installments    INT        NOT NULL,
            payment_value           FLOAT      NOT NULL
        );
        """
)

staging_reviews_table_create= (
        """
        CREATE TABLE staging_reviews 
        (
            review_id                  VARCHAR      NOT NULL,
            order_id                   VARCHAR      NOT NULL,
            review_score               INT          NOT NULL,
            review_comment_title       VARCHAR,
            review_comment_message     VARCHAR(999),
            review_creation_date       TIMESTAMP    NOT NULL,
            review_answer_timestamp    TIMESTAMP    NOT NULL
        );
        """
)

staging_orders_table_create= (
        """
        CREATE TABLE staging_orders 
        (
            order_id                         VARCHAR      NOT NULL,
            customer_id                      VARCHAR      NOT NULL,
            order_status                     VARCHAR      NOT NULL,
            order_purchase_timestamp         TIMESTAMP    NOT NULL,
            order_approved_at                TIMESTAMP,
            order_delivered_carrier_date     TIMESTAMP,
            order_delivered_customer_date    TIMESTAMP,
            order_estimated_delivery_date    TIMESTAMP    NOT NULL
        );
        """
)

staging_products_table_create= (
        """
        CREATE TABLE staging_products 
        (
            product_id                    VARCHAR    NOT NULL,
            product_category_name         VARCHAR,
            product_name_lenght           FLOAT,
            product_description_lenght    FLOAT,
            product_photos_qty            FLOAT,
            product_weight_g              FLOAT,
            product_length_cm             FLOAT,
            product_height_cm             FLOAT,
            product_width_cm              FLOAT  
        );
        """
)

staging_sellers_table_create= (
        """
        CREATE TABLE staging_sellers 
        (
            seller_id                 VARCHAR    NOT NULL,
            seller_zip_code_prefix    INT        NOT NULL,
            seller_city               VARCHAR    NOT NULL,
            seller_state              VARCHAR    NOT NULL
        );
        """
)

staging_closed_deals_table_create= (
        """
        CREATE TABLE staging_closed_deals 
        (
            mql_id                           VARCHAR      NOT NULL,
            seller_id                        VARCHAR      NOT NULL,
            sdr_id                           VARCHAR      NOT NULL,
            sr_id                            VARCHAR      NOT NULL,
            won_date                         TIMESTAMP    NOT NULL,
            business_segment                 VARCHAR,
            lead_type                        VARCHAR,
            lead_behaviour_profile           VARCHAR,
            has_company                      VARCHAR,
            has_gtin                         VARCHAR,
            average_stock                    VARCHAR,
            business_type                    VARCHAR,
            declared_product_catalog_size    FLOAT,
            declared_monthly_revenue         FLOAT        NOT NULL
        );
        """
)

staging_qualified_leads_table_create= (
        """
        CREATE TABLE staging_qualified_leads 
        (
            mql_id                VARCHAR      NOT NULL,
            first_contact_date    TIMESTAMP    NOT NULL,
            landing_page_id       VARCHAR      NOT NULL,
            origin                VARCHAR
        );
        """
)

dim_date_table_create= (
        """
        CREATE TABLE dim_date 
        (
            date_key      INTEGER     SORTKEY PRIMARY KEY,
            date          DATE        NOT NULL,
            year          SMALLINT    NOT NULL,
            quarter       SMALLINT    NOT NULL,
            month         SMALLINT    NOT NULL,
            day           SMALLINT    NOT NULL,
            week          SMALLINT    NOT NULL,
            is_weekend    BOOLEAN
        );
        """
)
    
dim_products_table_create= (
        """
        CREATE TABLE dim_products 
        (
            product_id               VARCHAR     SORTKEY PRIMARY KEY,
            product_category_name    VARCHAR     NOT NULL
        );
        """
)

dim_sellers_table_create= (
        """
        CREATE TABLE dim_sellers 
        (
            seller_id                 VARCHAR     SORTKEY PRIMARY KEY,
            seller_zip_code_prefix    VARCHAR     NOT NULL,
            seller_city               VARCHAR     NOT NULL,
            seller_state              VARCHAR     NOT NULL,
            seller_geo_lat            FLOAT       NOT NULL,
            seller_geo_lng            FLOAT       NOT NULL
        );
        """
)  


dim_payments_table_create= (
        """
        CREATE TABLE dim_payments
        (
            payment_key             INT IDENTITY(0,1)      NOT NULL PRIMARY KEY,
            order_id                VARCHAR     NOT NULL,
            payment_sequential      SMALLINT    NOT NULL,
            payment_type            VARCHAR     NOT NULL,
            payment_installments    SMALLINT    NOT NULL
        );
        """
)  

dim_customers_table_create= (
        """
        CREATE TABLE dim_customers 
        (
            customer_id                 VARCHAR     SORTKEY PRIMARY KEY,
            customer_zip_code_prefix    VARCHAR     NOT NULL,
            customer_city               VARCHAR     NOT NULL,
            customer_state              VARCHAR     NOT NULL,
            customer_geo_lat            FLOAT       NOT NULL,
            customer_geo_lng            FLOAT       NOT NULL
        );
        """
) 

fact_orders_table_create= (
        """
        CREATE TABLE fact_orders
        (
            order_id            VARCHAR     SORTKEY PRIMARY KEY,
            product_id          VARCHAR     NOT NULL,
            date_key            INTEGER     NOT NULL,
            seller_id           VARCHAR     NOT NULL,
            customer_id         VARCHAR     NOT NULL,
            freight_value       FLOAT       NOT NULL,
            price               FLOAT       NOT NULL,
            order_items_qtd     INTEGER     NOT NULL,
            order_status        VARCHAR     NOT NULL,
            FOREIGN KEY (product_id)     REFERENCES    dim_products(product_id),
            FOREIGN KEY (date_key)         REFERENCES    dim_date(date_key),
            FOREIGN KEY (seller_id)      REFERENCES    dim_sellers(seller_id),
            FOREIGN KEY (customer_id)    REFERENCES    dim_customers(customer_id)
        );
        
        ALTER TABLE dim_payments
        ADD FOREIGN KEY (order_id) REFERENCES fact_orders(order_id);
        """
)  


# STAGING TABLES

staging_customers_copy = (
    """
    COPY staging_customers 
    FROM '{}'
    iam_role '{}'
    IGNOREHEADER 1
    CSV
    """
).format(ECOMMERCE_DATA + 'olist_customers_dataset.csv', DWH_IAM_ROLE_ARN)

staging_geolocation_copy = (
    """
    COPY staging_geolocation
    FROM '{}'
    iam_role '{}'
    IGNOREHEADER 1
    CSV
    """
).format(ECOMMERCE_DATA + 'olist_geolocation_dataset.csv', DWH_IAM_ROLE_ARN)

staging_items_copy = (
    """
    COPY staging_items 
    FROM '{}'
    iam_role '{}'
    IGNOREHEADER 1
    CSV
    """
).format(ECOMMERCE_DATA + 'olist_order_items_dataset.csv', DWH_IAM_ROLE_ARN)

staging_payments_copy = (
    """
    COPY staging_payments 
    FROM '{}'
    iam_role '{}'
    IGNOREHEADER 1
    CSV
    """
).format(ECOMMERCE_DATA + 'olist_order_payments_dataset.csv', DWH_IAM_ROLE_ARN)

staging_reviews_copy = (
    """
    COPY staging_reviews 
    FROM '{}'
    iam_role '{}'
    IGNOREHEADER 1
    CSV
    """
).format(ECOMMERCE_DATA + 'olist_order_reviews_dataset.csv', DWH_IAM_ROLE_ARN)

staging_orders_copy = (
    """
    COPY staging_orders 
    FROM '{}'
    iam_role '{}'
    IGNOREHEADER 1
    CSV
    """
).format(ECOMMERCE_DATA + 'olist_orders_dataset.csv', DWH_IAM_ROLE_ARN)

staging_products_copy = (
    """
    COPY staging_products 
    FROM '{}'
    iam_role '{}'
    IGNOREHEADER 1
    CSV
    """
).format(ECOMMERCE_DATA + 'olist_products_dataset.csv', DWH_IAM_ROLE_ARN)

staging_sellers_copy = (
    """
    COPY staging_sellers 
    FROM '{}'
    iam_role '{}'
    IGNOREHEADER 1
    CSV
    """
).format(ECOMMERCE_DATA + 'olist_sellers_dataset.csv', DWH_IAM_ROLE_ARN)

staging_closed_deals_copy = (
    """
    COPY staging_closed_deals 
    FROM '{}'
    iam_role '{}'
    IGNOREHEADER 1
    CSV
    """
).format(MARKETING_FUNNEL_DATA + 'olist_closed_deals_dataset.csv', DWH_IAM_ROLE_ARN)

staging_qualified_leads_copy = (
    """
    COPY staging_qualified_leads 
    FROM '{}'
    iam_role '{}'
    IGNOREHEADER 1
    CSV
    """
).format(MARKETING_FUNNEL_DATA + 'olist_marketing_qualified_leads_dataset.csv', DWH_IAM_ROLE_ARN)

# FINAL TABLES

dim_date_table_insert = (
        """
        INSERT INTO dim_date (date_key, date, year, quarter, month, day, week, is_weekend)
        SELECT DISTINCT(TO_CHAR(order_purchase_timestamp :: DATE, 'yyyyMMDD')::integer) AS date_key,
               date(order_purchase_timestamp)                                           AS date,
               EXTRACT(year FROM order_purchase_timestamp)                              AS year,
               EXTRACT(quarter FROM order_purchase_timestamp)                           AS quarter,
               EXTRACT(month FROM order_purchase_timestamp)                             AS month,
               EXTRACT(day FROM order_purchase_timestamp)                               AS day,
               EXTRACT(week FROM order_purchase_timestamp)                              AS week,
               CASE WHEN EXTRACT(DOW FROM order_purchase_timestamp) IN (0, 6) THEN true ELSE false END AS is_weekend
        FROM staging_orders;
        """
)

dim_products_table_insert= (
        """
        INSERT INTO dim_products (product_id, product_category_name)
        SELECT product_id,
               product_category_name
        FROM staging_products;
        """
)

dim_sellers_table_insert= (
        """
        INSERT INTO dim_sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state, seller_geo_lat, seller_geo_lng)
        SELECT s.seller_id,
               s.seller_zip_code_prefix,
               s.seller_city,
               s.seller_state,
               g.geolocation_lat as seller_geo_lat,
               g.geolocation_lng as seller_geo_lng
        FROM staging_sellers s
        JOIN staging_geolocation g ON (s.seller_zip_code_prefix = g.geolocation_zip_code_prefix);
        """
)  

dim_payments_table_insert= (
        """
        INSERT INTO dim_payments (order_id, payment_sequential, payment_type, payment_installments)
        SELECT o.order_id,
               p.payment_sequential,
               p.payment_type,
               p.payment_installments
        FROM staging_payments p
        JOIN staging_orders o ON (p.order_id = o.order_id);
        """
)  

dim_customers_table_insert= (
        """
        INSERT INTO dim_customers (customer_id, customer_zip_code_prefix, customer_city, customer_state, customer_geo_lat, customer_geo_lng)
        SELECT c.customer_id,
               c.customer_zip_code_prefix,
               c.customer_city,
               c.customer_state,
               g.geolocation_lat as customer_geo_lat,
               g.geolocation_lng as customer_geo_lng
        FROM staging_customers c
        JOIN staging_geolocation g ON (c.customer_zip_code_prefix = g.geolocation_zip_code_prefix);
        """
) 

fact_orders_table_insert= (
        """
        INSERT INTO fact_orders (order_id, product_id, date_key, seller_id, customer_id, freight_value, price, order_items_qtd, order_status)
        SELECT DISTINCT o.order_id,
               p.product_id AS product_id,
               TO_CHAR(o.order_purchase_timestamp :: DATE, 'yyyyMMDD')::integer AS date_key,
               s.seller_id as seller_id,
               c.customer_id as customer_id,
               sum(i.freight_value),
               sum(i.price),
               count(*) as order_items_qtd,
               o.order_status
        FROM staging_orders o
            JOIN staging_items i ON (o.order_id = i.order_id)
            JOIN staging_products p ON (i.product_id = p.product_id)
            JOIN staging_sellers s ON (i.seller_id = s.seller_id)
            JOIN staging_customers c ON (o.customer_id = c.customer_id)
        GROUP BY o.order_id, p.product_id, TO_CHAR(o.order_purchase_timestamp :: DATE, 'yyyyMMDD')::integer, s.seller_id, c.customer_id, o.order_status;
        """
)  

# QUERY LISTS

create_table_queries = [staging_customers_table_create, staging_geolocation_table_create, staging_items_table_create, staging_payments_table_create, staging_reviews_table_create, staging_orders_table_create, staging_products_table_create, staging_sellers_table_create, staging_closed_deals_table_create, staging_qualified_leads_table_create, dim_date_table_create, dim_products_table_create, dim_sellers_table_create, dim_payments_table_create, dim_customers_table_create, fact_orders_table_create]

drop_table_queries = [staging_customers_table_drop, staging_geolocation_table_drop, staging_items_table_drop, staging_payments_table_drop, staging_reviews_table_drop, staging_orders_table_drop, staging_products_table_drop, staging_sellers_table_drop, staging_closed_deals_table_drop, staging_qualified_leads_table_drop, fact_orders_table_drop, dim_date_table_drop, dim_products_table_drop, dim_sellers_table_drop, dim_payments_table_drop, dim_customers_table_drop]

copy_table_queries = [staging_customers_copy, staging_geolocation_copy, staging_items_copy, staging_payments_copy, staging_reviews_copy, staging_orders_copy, staging_products_copy, staging_sellers_copy, staging_closed_deals_table_drop, staging_qualified_leads_copy]

insert_table_queries =[dim_date_table_insert, dim_products_table_insert, dim_sellers_table_insert, dim_payments_table_insert, dim_customers_table_insert, fact_orders_table_insert]