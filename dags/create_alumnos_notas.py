import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'UdeSA',
    'start_date': pendulum.datetime(2024, 3, 5, tz='UTC'),
}

# Este DAG consume las tablas "alumnos_programas" y "notas" y crea una tabla "alumnos_notas" con los datos de ambos.
# Siendo que es un MVP, la data es estatica y fue ingestada a la base de datos mediante Airbyte.

with DAG(
    dag_id="alumnos_notas",
    default_args=default_args,
    catchup=False,
    schedule_interval=None,  # Run manually,
    tags=['csv', 'postgres'],
) as dag:

    # Tarea para extraer datos de la tabla "alumnos_programas"
    extract_alumnos_programas = PostgresOperator(
        task_id="extract_alumnos_programas",
        postgres_conn_id="postgres_db",
        sql="""
            CREATE TABLE IF NOT EXISTS tmp_alumnos_programas(
                N_ID_PERSONA numeric(12), 
                N_ID_ALU_PROG numeric(12),
                C_IDENTIFICACION numeric(12),
                C_PROGRAMA numeric(12),
                C_ORIENTACION numeric(12),
                C_PLAN numeric(12)
            );
            insert into tmp_alumnos_programas (N_ID_PERSONA, N_ID_ALU_PROG, C_IDENTIFICACION, C_PROGRAMA, C_ORIENTACION, C_PLAN)
            SELECT "N_ID_PERSONA", "N_ID_ALU_PROG", "C_IDENTIFICACION", "C_PROGRAMA", "C_ORIENTACION", "C_PLAN" FROM alumnos_programas;
        """,
    )

    # Tarea para extraer datos de la tabla "notas"
    extract_notas = PostgresOperator(
        task_id="extract_notas",
        postgres_conn_id="postgres_db",
        sql="""
            CREATE TABLE IF NOT EXISTS tmp_notas(
                N_ID_ALU_PROG numeric(12),
                N_ID_MATERIA numeric(12), 
                M_APRUEBA_MAT VARCHAR,
                F_RINDE VARCHAR
            );
            insert into tmp_notas (N_ID_MATERIA, M_APRUEBA_MAT, F_RINDE, N_ID_ALU_PROG)
            SELECT "N_ID_MATERIA", "M_APRUEBA_MAT", "F_RINDE" , "N_ID_ALU_PROG" FROM notas;
        """,
    )

    # Tarea para combinar las dos tablas
    combine_data = PostgresOperator(
        task_id="combine_data",
        postgres_conn_id="postgres_db",
        sql="""
            create TABLE IF NOT EXISTS alumnos_notas(
                N_ID_PERSONA numeric(12), 
                N_ID_ALU_PROG numeric(12),
                C_IDENTIFICACION numeric(12),
                C_PROGRAMA numeric(12),
                C_ORIENTACION numeric(12),
                C_PLAN numeric(12),
                N_ID_MATERIA numeric(12), 
                M_APRUEBA_MAT VARCHAR,
                F_RINDE VARCHAR
            );
            INSERT INTO alumnos_notas (N_ID_PERSONA, N_ID_ALU_PROG, C_IDENTIFICACION, C_PROGRAMA, C_ORIENTACION, C_PLAN, N_ID_MATERIA, M_APRUEBA_MAT, F_RINDE)
            SELECT
                a.N_ID_PERSONA,
                a.N_ID_ALU_PROG,
                a.C_IDENTIFICACION,
                a.C_PROGRAMA,
                a.C_ORIENTACION,
                a.C_PLAN,
                n.N_ID_MATERIA,
                n.M_APRUEBA_MAT,
                n.F_RINDE
            FROM tmp_alumnos_programas a
            INNER JOIN tmp_notas n ON a.N_ID_ALU_PROG = n.N_ID_ALU_PROG;
        """
    )

    delete_temp_files = PostgresOperator(
        task_id="delete_temp_files",
        postgres_conn_id="postgres_db",
        sql="DROP TABLE IF EXISTS tmp_alumnos_programas, tmp_notas;"
    )

    # Dependencias entre tareas
    extract_alumnos_programas >> extract_notas >> combine_data >> delete_temp_files
