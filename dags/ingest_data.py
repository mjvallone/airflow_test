import pendulum
from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Define default arguments
default_args = {
    'owner': 'tincho',
    'start_date': pendulum.datetime(2024, 3, 5, tz='UTC'),
}

# Define DAG
with DAG(
    dag_id='csv_to_postgres_local',
    default_args=default_args,
    catchup=False,
    schedule_interval=None,  # Run manually,
    tags=['csv', 'postgres'],
) as dag:
    # FIXME lo podríamos dejar como ejemplo
    create_table = PostgresOperator(
        task_id='create_table_students',
        postgres_conn_id='postgres_db',
        sql="""
        CREATE TABLE IF NOT EXISTS students (
            id VARCHAR(255) PRIMARY KEY,
            first_name VARCHAR(255) NOT NULL,
            last_name VARCHAR(255) NOT NULL,
            date_of_birth DATE,
            ethnicity VARCHAR(255),
            gender VARCHAR(255),
            status VARCHAR(255),
            entry_academic_period VARCHAR(255),
            exclusion_type VARCHAR(255),
            act_composite INTEGER,
            act_math INTEGER,
            act_english INTEGER,
            act_reading INTEGER,
            sat_combined INTEGER,
            sat_math INTEGER,
            sat_verbal INTEGER,
            sat_reading INTEGER,
            hs_gpa DECIMAL(3,2), -- Consider storing GPA with two decimal places
            hs_city VARCHAR(255),
            hs_state VARCHAR(255),
            hs_zip VARCHAR(255),
            email VARCHAR(255),
            entry_age INTEGER,
            ged VARCHAR(255),
            english_2nd_language BOOLEAN, -- Use BOOLEAN for true/false values
            first_generation BOOLEAN
        );
        """,
    )

    # create_table = PostgresOperator(
    #     task_id='create_table_terms',
    #     postgres_conn_id='postgres_db',
    #     sql="""
    #     CREATE TABLE IF NOT EXISTS terms (
    #         id PRIMARY KEY, -- Use SERIAL for auto-incrementing integer ID
    #         program_code VARCHAR(255) NOT NULL,
    #         status VARCHAR(255) NOT NULL,
    #         academic_period VARCHAR(255) NOT NULL,
    #         fulltime BOOLEAN, -- Use BOOLEAN for true/false values
    #         pell_eligible BOOLEAN, -- Use BOOLEAN for true/false values
    #         cumulative_gpa DECIMAL(5,2), -- Consider storing GPA with two decimal places
    #         cumulative_points INTEGER,
    #         cumulative_credit_hours INTEGER,
    #         semester_gpa DECIMAL(5,2), -- Consider storing GPA with two decimal places
    #         semester_points INTEGER,
    #         semester_credit_hours INTEGER
    #     );
    #     """,
    # )

    load_data_students = PostgresOperator(
        task_id='load_data_students',
        postgres_conn_id='postgres_db',
        sql="COPY students FROM '/home/tincho/dev/airflow_test/data/students.csv' DELIMITER ',' CSV HEADER",
    )

    # load_data_terms = PostgresOperator(
    #     task_id='load_data_terms',
    #     postgres_conn_id='postgres_db',
    #     sql="COPY terms FROM '/data/terms.csv' DELIMITER ',' CSV HEADER",
    # )

    # Set dependencies
    create_table >> load_data_students
    # create_table >> load_data_terms
