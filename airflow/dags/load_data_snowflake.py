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
    task_id='load_and_merge_customer_customer_demo',
    sql="""
    USE DATABASE northwind;

    CREATE OR REPLACE TEMPORARY TABLE staging_customer_customer_demo LIKE raw.customer_customer_demo;

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

load_data = SnowflakeOperator(
    task_id='load_and_merge_customer_demographics',
    sql="""
    USE DATABASE northwind;

    CREATE OR REPLACE TEMPORARY TABLE staging_customer_demographics LIKE raw.customer_demographics;

    -- Carregar dados na tabela de staging
    COPY INTO staging_customer_demographics
    FROM 's3://desafio-indicium/northwind/customer_customer_demographics.csv'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_DELIMITER = ';');

    -- Inserir dados únicos na tabela final
    MERGE INTO raw.customer_demographics AS target
    USING staging_customer_demographics AS source
    ON target.customer_type_id = source.customer_type_id
    WHEN MATCHED THEN
    UPDATE SET
        target.customer_type_id = source.customer_type_id,
        target.customer_desc = source.customer_desc
    WHEN NOT MATCHED THEN
    INSERT (customer_type_id, customer_desc) VALUES (source.customer_type_id, source.customer_desc
    );
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

load_data = SnowflakeOperator(
    task_id='load_and_merge_employees',
    sql="""
      USE DATABASE northwind;

    CREATE OR REPLACE TEMPORARY TABLE staging_employees LIKE raw.employees;

    -- Carregar dados na tabela de staging
    COPY INTO staging_employees
    FROM 's3://desafio-indicium/northwind/employees.csv'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_DELIMITER = ';');

    MERGE INTO raw.employees AS target
    USING staging_employees AS source
    ON target.employee_id = source.employee_id
    WHEN MATCHED THEN
        UPDATE SET
            target.last_name = source.last_name,
            target.first_name = source.first_name,
            target.title = source.title,
            target.title_of_courtesy = source.title_of_courtesy, -- Corrigido o nome da coluna
            target.birth_date = source.birth_date,
            target.hire_date = source.hire_date,
            target.address = source.address,
            target.city = source.city,
            target.region = source.region,
            target.postal_code = source.postal_code,
            target.country = source.country,
            target.home_phone = source.home_phone,
            target.extension = source.extension,
            target.photo = source.photo,
            target.notes = source.notes,
            target.reports_to = source.reports_to,
            target.photo_path = source.photo_path
    WHEN NOT MATCHED THEN
        INSERT (employee_id, last_name, first_name, title, title_of_courtesy, birth_date, hire_date, address, city, region, postal_code, country, home_phone, extension, photo, notes, reports_to, photo_path)
        VALUES (source.employee_id, source.last_name, source.first_name, source.title, source.title_of_courtesy, source.birth_date, source.hire_date, source.address, source.city, source.region, source.postal_code, source.country, source.home_phone, source.extension, source.photo, source.notes, source.reports_to, source.photo_path);
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)



load_data 