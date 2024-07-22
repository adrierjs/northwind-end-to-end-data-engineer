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
    'norhwind_clean',
    default_args=default_args,
    schedule_interval='30 0 * * *',
    start_date=days_ago(1),
    catchup=False,
)

norhwind_clean = SnowflakeOperator(
    task_id='snowflake_clean_categories',
    sql="""
    create or replace table clean.northwing.categories CLUSTER BY (category_id) copy grants as 
    select category_id, upper(category_name) category_name, 
    upper(description) description, picture from northwind.raw.categories;
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

norhwind_clean = SnowflakeOperator(
    task_id='snowflake_clean_customers',
    sql="""
    create or replace table clean.northwing.customers CLUSTER BY (customer_id) copy grants as 
    select
        upper(customer_id) as customer_id,
        upper(company_name) as company_name,
        upper(contact_name) as contact_name,
        upper(contact_title) as contact_title,
        upper(address) as address,
        upper(city) as city,
        upper(region) as region,
        upper(postal_code) as postal_code,
        upper(country) as country,
        upper(phone) as phone,
        upper(fax) as fax
    from northwind.raw.customers;
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

norhwind_clean = SnowflakeOperator(
    task_id='snowflake_clean_customer_customer_demo',
    sql="""
    create or replace table clean.northwing.customer_customer_demo CLUSTER BY (customer_id) copy grants as 
    select
        upper(category_id) as category_id,
        upper(customer_type_id) as customer_type_id
    from northwind.raw.customer_customer_demo;
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

norhwind_clean = SnowflakeOperator(
    task_id='snowflake_clean_customer_demographics',
    sql="""
    create or replace table clean.northwing.customer_customer_demo CLUSTER BY (customer_id) copy grants as 
    select
        upper(customer_type_id) as customer_type_id,
        upper(customer_desc) as customer_desc
    from northwind.raw.customer_demographics;
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

norhwind_clean
