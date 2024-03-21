import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'UdeSA',
    'start_date': pendulum.datetime(2024, 3, 13, tz='UTC'),
}

# Este DAG consume las tablas "alumnos_programas" y "calendario" y agrega a la tabla "alumnos_programas" los campos referidos a la fecha de inicio de una persona.

with DAG(
    dag_id="add_fecha_inicio_to_alumnos_programas",
    default_args=default_args,
    catchup=False,
    schedule_interval=None,  # Run manually,
    tags=['csv', 'postgres'],
) as dag:

    # Tarea para extraer la fecha de inicio
    extract_fecha_inicio = PostgresOperator(
        task_id="extract_fecha_inicio",
        postgres_conn_id="postgres_db",
        sql="""
            CREATE TABLE IF NOT EXISTS tmp_fecha_inicio(
                n_id_alu_prog numeric(12),
                fecha_inicio varchar
            );
            insert into tmp_fecha_inicio (n_id_alu_prog, fecha_inicio)
            SELECT
                a."N_ID_ALU_PROG",
                c.f_inicio
            FROM alumnos_programas a
            INNER JOIN calendario c ON a."N_ID_CALEN_CURSADA" = c.n_id_calen_cursada;
        """,        
    )

   # Tarea para agregar la fecha de inicio
    add_fecha_inicio = PostgresOperator(
        task_id="add_fecha_inicio",
        postgres_conn_id="postgres_db",
        sql="""
            ALTER TABLE alumnos_programas
            ADD COLUMN IF NOT EXISTS fecha_inicio varchar;

            UPDATE alumnos_programas a
            SET fecha_inicio = t.fecha_inicio
            FROM tmp_fecha_inicio t
            WHERE a."N_ID_ALU_PROG" = t.n_id_alu_prog;
        """,
    )


    delete_temp_files = PostgresOperator(
        task_id="delete_temp_files",
        postgres_conn_id="postgres_db",
        sql="DROP TABLE IF EXISTS tmp_fecha_inicio;"
    )

    # Dependencias entre tareas
    extract_fecha_inicio >> add_fecha_inicio >> delete_temp_files
