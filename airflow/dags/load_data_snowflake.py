from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

# Define a DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
    # 'retries': 1,
}

dag = DAG(
    'load_data_snowflake',
    default_args=default_args,
    schedule_interval=None, 
    start_date=days_ago(1),
    catchup=False,
)

load_data = SnowflakeOperator(
    task_id='load_and_merge_categories',
    sql="""

    USE DATABASE northwind;
    CREATE OR REPLACE TEMPORARY TABLE staging_categories LIKE raw.categories;

    -- Criação de tabela temporária 
    COPY INTO staging_categories
    FROM 's3://desafio-indicium/northwind/categories.csv'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_DELIMITER = ';');

    MERGE INTO raw.categories AS target
    USING staging_categories AS source
    ON target.category_id = source.category_id
    WHEN MATCHED THEN
    UPDATE SET
        target.category_name = source.category_name,
        target.description = source.description,
        target.picture = source.picture
    WHEN NOT MATCHED THEN
    INSERT (category_id, category_name, description, picture) VALUES (source.category_id, source.category_name,
    source.description, source.picture);
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

load_data = SnowflakeOperator(
    task_id='load_and_merge_customers',
    sql="""
    USE DATABASE northwind;
    CREATE OR REPLACE TEMPORARY TABLE staging_customers LIKE raw.customers;

    COPY INTO staging_customers
    FROM 's3://desafio-indicium/northwind/customers.csv'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_DELIMITER = ';');

    MERGE INTO raw.customers AS target
        USING staging_customers AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN
        UPDATE SET
            target.company_name = source.company_name,
            target.contact_name = source.contact_name,
            target.contact_title = source.contact_title,
            target.address = source.address,
            target.city = source.city,
            target.region = source.region,
            target.postal_code = source.postal_code,
            target.country = source.country,
            target.phone = source.phone,
            target.fax = source.fax
            
            
        WHEN NOT MATCHED THEN
        INSERT (customer_id, company_name, contact_name, contact_title,
        address, city, region, postal_code, country, phone, fax
        
        ) VALUES (customer_id, company_name, contact_name, contact_title,
        address, city, region, postal_code, country, phone, fax
        );
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

load_data = SnowflakeOperator(
    task_id='load_and_merge_categories',
    sql="""

    USE DATABASE northwind;
    CREATE OR REPLACE TEMPORARY TABLE staging_categories LIKE raw.categories;

    -- Criação de tabela temporária 
    COPY INTO staging_categories
    FROM 's3://desafio-indicium/northwind/categories.csv'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_DELIMITER = ';');

    MERGE INTO raw.categories AS target
    USING staging_categories AS source
    ON target.category_id = source.category_id
    WHEN MATCHED THEN
    UPDATE SET
        target.category_name = source.category_name,
        target.description = source.description,
        target.picture = source.picture
    WHEN NOT MATCHED THEN
    INSERT (category_id, category_name, description, picture) VALUES (source.category_id, source.category_name,
    source.description, source.picture);
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

load_data = SnowflakeOperator(
    task_id='load_and_merge_customer_customer_demo',
    sql="""
    USE DATABASE northwind;

    CREATE OR REPLACE TEMPORARY TABLE staging_customer_demo LIKE raw.customer_customer_demo;

    -- Carregar dados na tabela de staging
    COPY INTO staging_customer_customer_demo
    FROM 's3://desafio-indicium/northwind/customer_customer_demo.csv'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_DELIMITER = ';');

    -- Inserir dados únicos na tabela final
    MERGE INTO raw.customer_customer_demo AS target
    USING staging_customer_customer_demo AS source
    ON target.category_id = source.category_id
    WHEN MATCHED THEN
    UPDATE SET
        target.category_id = source.category_id,
        target.customer_type_id = source.customer_type_id
    WHEN NOT MATCHED THEN
    INSERT (category_id, customer_type_id) VALUES (source.category_id, source.customer_type_id
    );
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

load_data 