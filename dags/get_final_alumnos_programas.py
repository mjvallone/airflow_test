import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'UdeSA',
    'start_date': pendulum.datetime(2024, 3, 21, tz='UTC'),
}

postgres_conn = "postgresql://postgres:postgres@localhost:5432/udesa"

#FIXME
# Este DAG consume las tablas "alumnos_programas" y "calendario" y agrega a la tabla "alumnos_programas" los campos referidos a la fecha de inicio de una persona.

query_alumnos_programas = """
    SELECT ap."N_ID_PERSONA", ap."N_PROMOCION", ap."C_BAJA", ap."F_BAJA", ap."C_IDENTIFICACION", ap."C_PROGRAMA", ap."C_ORIENTACION", pg.c_plan, pm."N_ID_MATERIA", 
    pm."N_ANO_CARRERA" as ano,
    CASE WHEN CAST(MOD(CAST(an.f_rinde as date) - CAST(ap.fecha_inicio AS date),365) AS decimal)/365 > 0.5 THEN 2 ELSE 1 END AS semestre,
    mpc.cantidad_materias AS cant_mat_requeridas
    FROM alumnos_programas ap
    LEFT OUTER JOIN planes_grupos pg   
    ON ap."C_IDENTIFICACION" = pg.c_identificacion AND ap."C_PROGRAMA" = pg.c_programa AND ap."C_ORIENTACION" = pg.c_orientacion AND ap."C_PLAN" = pg.c_plan
    INNER JOIN planes_materias pm 
    ON ap."C_IDENTIFICACION" = pm."C_IDENTIFICACION"  AND ap."C_PROGRAMA" = pm."C_PROGRAMA" AND ap."C_ORIENTACION" = pm."C_ORIENTACION" AND ap."C_PLAN" = pm."C_PLAN" AND pg.n_grupo = pm."N_GRUPO"
    LEFT OUTER JOIN alumnos_notas an
    ON ap."N_ID_PERSONA" = an.n_id_persona AND ap."N_ID_ALU_PROG" = an.n_id_alu_prog AND ap."C_IDENTIFICACION" = an.c_identificacion  AND ap."C_PROGRAMA" = an.c_programa AND ap."C_ORIENTACION" = an.c_orientacion AND pm."N_ID_MATERIA" = an.n_id_materia
    LEFT OUTER JOIN materias_por_carrera mpc 
    ON ap."C_IDENTIFICACION" = mpc.identificacion AND ap."C_PROGRAMA" = mpc.programa AND ap."C_ORIENTACION" = mpc.orientacion AND ap."C_PLAN" = mpc.plan 
    AND mpc.ano = pm."N_ANO_CARRERA" 
    AND mpc.semestre = CASE WHEN CAST(MOD(CAST(an.f_rinde AS date) - CAST(ap.fecha_inicio AS date),365) as decimal)/365 > 0.5 THEN 2 ELSE 1 END
    WHERE ap."F_GRADUACION" IS NULL AND pm."N_ANO_CARRERA" <= ((current_date - CAST(ap.fecha_inicio AS date))/365)+1 --limitamos la cantidad de registros para que sea mas liviano
    AND ap."N_ID_PERSONA" = 200572  --in (229632, 170434, 200572, 175447)
    AND an.m_aprueba_mat ='Si'
    ORDER BY pm."N_ANO_CARRERA", semestre
"""

query_planes_materias = """
    SELECT pm."C_IDENTIFICACION" , pm."C_PROGRAMA" , pm."C_ORIENTACION" , pm."C_PLAN", pm."N_ID_MATERIA"
    FROM planes_materias pm
"""


def process_data():
        pg_hook = PostgresHook(postgres_conn_id="postgres_db")
        alumnos_programa = pg_hook.get_pandas_df(sql=query_alumnos_programas)
        planes_materias = pg_hook.get_pandas_df(sql=query_planes_materias)

        #obtengo las materias que son de una carrera en la cual la persona no está mas (se cambió de carrera)
        subjects_to_check = alumnos_programa[alumnos_programa['C_BAJA'] == "CC"]
        subjects_to_check["found_in_plans_materias"] = False

        # obtengo la carrera actual para las personas que se cambiaron de carrera
        filtered_df = alumnos_programa[(alumnos_programa['C_BAJA'].isnull()) & (alumnos_programa["N_ID_PERSONA"].isin(subjects_to_check["N_ID_PERSONA"]))]
        # me quedo con una row por N_ID_PERSONA
        current_degree_for_people_with_cc = filtered_df.drop_duplicates(subset=["N_ID_PERSONA"])
        print(current_degree_for_people_with_cc)

        # reviso cuales de las materias de la carrera previa, deben ser consideradas para la carrera actual
        for index, row in subjects_to_check.iterrows():
            print(f"row: {row}")
            print('-------------------------')
            persona_row = current_degree_for_people_with_cc[current_degree_for_people_with_cc["N_ID_PERSONA"] == row["N_ID_PERSONA"]]
            print(f"persona_row: {persona_row}")
            current_identificacion, current_programa, current_orientacion, current_plan = persona_row[["C_IDENTIFICACION"], ["C_PROGRAMA"], ["C_ORIENTACION"], ["C_PLAN"]]
            subject_to_check = row["N_ID_MATERIA"]
            print(current_identificacion, current_programa, current_orientacion, current_plan, subject_to_check)
            if (planes_materias[planes_materias["C_IDENTIFICACION"] == current_identificacion & planes_materias["C_PROGRAMA"] == current_programa & planes_materias["C_ORIENTACION"] == current_orientacion & planes_materias["C_PLAN"] == current_plan & planes_materias["N_ID_MATERIA"] == subjects_to_check]):
                # buscar en planes_materias la materia, si la encuentra nos quedamos con la materia, sino FALSE
                row["found_in_plans_materias"] = True
        
        print(subjects_to_check.head())

        print("3")
        # filtro las materias que estan en la carrera actual de la persona
        valid_subjects = subjects_to_check[subjects_to_check["found_in_plans_materias"] == True]
        # quito de alumnos_programa las rows con las materias que no corresponden o son de una carrera dada de baja
        filtered_alumnos_programa = alumnos_programa[(alumnos_programa["C_BAJA"] == "CC" & alumnos_programa["N_ID_MATERIA"].isin(valid_subjects["N_ID_MATERIA"])) | alumnos_programa["F_BAJA"] is None]

        print("4")
        # elimino el campo c_baja
        filtered_alumnos_programa = filtered_alumnos_programa.drop(columns=["C_BAJA", "F_BAJA"])
        
        print("5")
        # falta la suma de las materias aprobadas
        grouped_alumnos_programa = filtered_alumnos_programa.groupby([
            "N_ID_PERSONA",
            "N_PROMOCION",
            "C_IDENTIFICACION",
            "C_PROGRAMA",
            "C_ORIENTACION",
            "c_plan",
            "N_ID_MATERIA",
            "ano",
            "semestre",
            "cant_mat_requeridas"
        ]).size().reset_index(name='cant_mat_aprobadas')        
        grouped_alumnos_programa["estado"] = grouped_alumnos_programa["cant_mat_aprobadas"] - grouped_alumnos_programa["cant_mat_requeridas"]

        print("6")
        # grouped_alumnos_programa.to_sql("datita", postgres_conn, if_exists="replace", index=False) #FIXME


with DAG(
    dag_id="get_final_alumnos_programas", #FIXME
    default_args=default_args,
    catchup=False,
    schedule_interval=None,  # Run manually,
    tags=['csv', 'postgres'],
) as dag:

#     # Tarea para obtener alumnos_programas
#     get_alumnos_programas = PostgresOperator(
#         task_id="get_alumnos_programas",
#         postgres_conn_id="postgres_db",
#         sql=query_alumnos_programas,        
#     )

#    # Tarea para obtener planes de materias
#     get_planes_materias = PostgresOperator(
#         task_id="get_planes_materias",
#         postgres_conn_id="postgres_db",
#         sql=query_planes_materias,
#     )

    process_data_task = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
        # op_args=[get_alumnos_programas, get_planes_materias],
    )

# tarea5 generar la data condensada, donde hace una row por persona-carrera

    # Dependencias entre tareas
    process_data_task
    # get_alumnos_programas >> process_data_task
    # get_alumnos_programas >> process_data_task
