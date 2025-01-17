from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

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
    create or replace table clean.northwind.categories CLUSTER BY (category_id) copy grants as 
    select 
        category_id, 
        upper(category_name) category_name, 
        upper(description) description,
        picture 
    from northwind.raw.categories;
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

norhwind_clean = SnowflakeOperator(
    task_id='snowflake_clean_customers',
    sql="""
    create or replace table clean.northwind.customers CLUSTER BY (customer_id) copy grants as 
    select
        customer_id,
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
    task_id='snowflake_clean_customer_demographics',
    sql="""
    create or replace table clean.northwind.customer_customer_demo CLUSTER BY (customer_type_id) copy grants as 
    select
       customer_type_id,
        upper(customer_desc) as customer_desc
    from northwind.raw.customer_demographics;
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

norhwind_clean = SnowflakeOperator(
    task_id='snowflake_clean_employees',
    sql="""
    create or replace table clean.northwind.employees CLUSTER BY (employee_id) copy grants as 
    select
        employee_id,
        upper(last_name) as last_name,
        upper(first_name) as first_name,
        upper(title) as title,
        upper(title_of_courtesy) as title_of_courtesy,
        birth_date,
        hire_date,
        upper(address) as address,
        upper(city) as city,
        upper(region) as region,
        upper(postal_code) as postal_code,
        upper(country) as country,
        upper(home_phone) as home_phone,
        upper(extension) as extension,
        upper(photo) as photo,
        upper(notes) as notes,
        reports_to,
        upper(photo_path) as photo_path
    from northwind.raw.employees;
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

norhwind_clean = SnowflakeOperator(
    task_id='snowflake_clean_employee_territories',
    sql="""
    create or replace table clean.northwind.employee_territories CLUSTER BY (employee_id) copy grants as 
    select
        employee_id,
        territory_id
    from northwind.raw.employee_territories;
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

norhwind_clean = SnowflakeOperator(
    task_id='snowflake_clean_orders',
    sql="""
    create or replace table clean.northwind.orders CLUSTER BY (order_id) copy grants as 
    select
        order_id,
        upper(customer_id) as customer_id,
        employee_id,
        order_date,
        required_date,
        shipped_date,
        ship_via,
        freight,
        upper(ship_name) as ship_name,
        upper(ship_address) as ship_address,
        upper(ship_city) as ship_city,
        upper(ship_region) as ship_region,
        upper(ship_postal_code) as ship_postal_code,
        upper(ship_country) as ship_country
    from northwind.raw.orders;
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

norhwind_clean = SnowflakeOperator(
    task_id='snowflake_clean_orders_values',
    sql="""
    create or replace table clean.northwind.orders_values CLUSTER BY (order_id) copy grants as 
    select 
        orders.order_id, 
        orders.customer_id, 
        sum(orders.freight) freight, 
        sum(order_details.unit_price) unit_price,
        sum(order_details.quantity) quantity, 
        sum(order_details.discount) discount 
    from clean.northwind.orders 
    inner join clean.northwind.order_details on order_details.order_id = orders.order_id
    group by 1,2
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

norhwind_clean = SnowflakeOperator(
    task_id='snowflake_clean_order_details',
    sql="""
    create or replace table clean.northwind.order_details CLUSTER BY (order_id) copy grants as 
    select
        order_id,
        product_id,
        unit_price,
        quantity,
        discount
    from northwind.raw.order_details;
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

norhwind_clean = SnowflakeOperator(
    task_id='snowflake_clean_order_products',
    sql="""
    create or replace table clean.northwind.products CLUSTER BY (product_id) copy grants as 
    select
        product_id,
        upper(product_name) as product_name,
        supplier_id,
        category_id,
        upper(quantity_per_unit) as quantity_per_unit,
        unit_price,
        units_in_stock,
        units_on_order,
        reorder_level,
        case 
            when discontinued = '0' then 'YES'
            when discontinued = '1' then 'NO'
            else discontinued::text
        end as discontinued
    from northwind.raw.products;
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

norhwind_clean = SnowflakeOperator(
    task_id='snowflake_clean_region',
    sql="""
    create or replace table clean.northwind.products CLUSTER BY (region_id) copy grants as 
    select
        region_id,
        upper(region_description) as region_description
    from northwind.raw.region;
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

norhwind_clean = SnowflakeOperator(
    task_id='snowflake_clean_shippers',
    sql="""
    create or replace table clean.northwind.shippers CLUSTER BY (shipper_id) copy grants as 
    select
        shipper_id,
        upper(company_name) as company_name,
        upper(phone) as phone
    from northwind.raw.shippers;
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

norhwind_clean = SnowflakeOperator(
    task_id='snowflake_clean_suppliers',
    sql="""
    create or replace table clean.northwind.suppliers CLUSTER BY (supplier_id) copy grants as 
    select
        supplier_id,
        upper(company_name) as company_name,
        upper(contact_name) as contact_name,
        upper(contact_title) as contact_title,
        upper(address) as address,
        upper(city) as city,
        upper(region) as region,
        upper(postal_code) as postal_code,
        upper(country) as country,
        phone,
        fax,
        upper(homepage) as homepage
    from northwind.raw.suppliers;
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

norhwind_clean = SnowflakeOperator(
    task_id='snowflake_clean_territories',
    sql="""
    create or replace table clean.northwind.territories CLUSTER BY (territory_id) copy grants as 
    select
        territory_id,
        upper(territory_description) as territory_description,
        region_id
    from northwind.raw.territories;
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

norhwind_clean = SnowflakeOperator(
    task_id='snowflake_clean_us_states',
    sql="""
    create or replace table clean.northwind.us_states CLUSTER BY (state_id) copy grants as 
    select
        state_id,
        upper(state_name) as state_name,
        upper(state_abbr) as state_abbr,
        upper(state_region) as state_region
    from northwind.raw.us_states;
    """,
    snowflake_conn_id='snowflake_connection',
    dag=dag,
)

norhwind_clean
