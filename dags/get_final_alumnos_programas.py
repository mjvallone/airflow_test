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

#FIXME
# Este DAG consume las tablas "alumnos_programas" y "calendario" y agrega a la tabla "alumnos_programas" los campos referidos a la fecha de inicio de una persona.

query_alumnos_programas = """
    SELECT ap."N_ID_PERSONA", ap."N_ID_ALU_PROG", ap."N_PROMOCION", ap."C_BAJA", ap."F_BAJA", ap."C_IDENTIFICACION", ap."C_PROGRAMA", ap."C_ORIENTACION", pg.c_plan, pm."N_ID_MATERIA", 
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
    AND ap."N_ID_PERSONA" in (229632, 170434, 200572, 175447)
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
        planes_materias["unique_key"] = (planes_materias["C_IDENTIFICACION"].astype(int).astype(str) +
                                 planes_materias["C_PROGRAMA"].astype(int).astype(str) +
                                 planes_materias["C_ORIENTACION"].astype(int).astype(str) +
                                 planes_materias["C_PLAN"].astype(int).astype(str) +
                                 planes_materias["N_ID_MATERIA"].astype(int).astype(str)
                                ).astype(int)

        #obtengo las materias que son de una carrera en la cual la persona no está mas (se cambió de carrera)
        subjects_to_check = alumnos_programa[alumnos_programa['C_BAJA'] == "CC"]
        subjects_to_check["found_in_plans_materias"] = False

        # obtengo la carrera actual para las personas que se cambiaron de carrera
        filtered_df = alumnos_programa[(alumnos_programa['C_BAJA'].isnull()) & (alumnos_programa["N_ID_PERSONA"].isin(subjects_to_check["N_ID_PERSONA"]))]
        # me quedo con una row por N_ID_PERSONA
        current_degree_for_people_with_cc = filtered_df.drop_duplicates(subset=["N_ID_PERSONA"])

        # reviso cuales de las materias de la carrera previa, deben ser consideradas para la carrera actual
        for index, row in subjects_to_check.iterrows():
            persona_row = current_degree_for_people_with_cc[current_degree_for_people_with_cc["N_ID_PERSONA"] == row["N_ID_PERSONA"]]
            current_data = persona_row[["C_IDENTIFICACION", "C_PROGRAMA", "C_ORIENTACION", "c_plan"]]
            subject_id_to_check = row["N_ID_MATERIA"]

            key_to_find = int(f'{int(current_data["C_IDENTIFICACION"].values[0])}{int(current_data["C_PROGRAMA"].values[0])}{int(current_data["C_ORIENTACION"].values[0])}{int(current_data["c_plan"].values[0])}{int(subject_id_to_check)}')

            if (planes_materias[planes_materias["unique_key"] == key_to_find]).empty == False:
                # busco la materia de la carrera anterior en planes_materias de la carrera actual
                 subjects_to_check.at[index, "found_in_plans_materias"] = True

        # filtro las materias que estan en la carrera actual de la persona
        valid_subjects = subjects_to_check[subjects_to_check["found_in_plans_materias"] == True]

        # quito de alumnos_programa las rows con las materias que no corresponden o son de una carrera dada de baja
        filtered_alumnos_programa = alumnos_programa[((alumnos_programa["C_BAJA"] == "CC") & (alumnos_programa["N_ID_MATERIA"].isin(valid_subjects["N_ID_MATERIA"]))) | (alumnos_programa["F_BAJA"].isna())]

        # elimino el campo c_baja y f_baja
        filtered_alumnos_programa = filtered_alumnos_programa.drop(columns=["C_BAJA", "F_BAJA"])
        
        # falta la suma de las materias aprobadas
        grouped_alumnos_programa = filtered_alumnos_programa.groupby([
            "N_ID_PERSONA",
            "N_PROMOCION",
            "c_plan",
            "ano",
            "semestre",
            "cant_mat_requeridas"
        ]).size().reset_index(name='cant_mat_aprobadas')        
        grouped_alumnos_programa["estado"] = grouped_alumnos_programa["cant_mat_aprobadas"] - grouped_alumnos_programa["cant_mat_requeridas"]
        print(grouped_alumnos_programa)


        grouped_by_alumnos = grouped_alumnos_programa.groupby([
            "N_ID_PERSONA",
        ]).agg(
             tot_cant_mat_req=('cant_mat_requeridas', 'sum'),
             tot_cant_mat_apr=('cant_mat_aprobadas', 'sum')
        ).reset_index()
        grouped_by_alumnos["estado"] = grouped_by_alumnos["tot_cant_mat_apr"] - grouped_by_alumnos["tot_cant_mat_req"]
        print(grouped_by_alumnos)

        grouped_alumnos_programa.to_csv("grouped_alumnos_programa.csv", index=False)
        grouped_by_alumnos.to_csv("grouped_by_alumnos.csv", index=False)

        #FIXME revisar que año 3 semestre 1 toma 5 materias requeridas y son 4, esta tomando del IPO de la carrera previa


with DAG(
    dag_id="get_final_alumnos_programas", #FIXME
    default_args=default_args,
    catchup=False,
    schedule_interval=None,  # Run manually,
    tags=['csv', 'postgres'],
) as dag:
    process_data_task = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

# tarea5 generar la data condensada, donde hace una row por persona-carrera

    # Dependencias entre tareas
    process_data_task