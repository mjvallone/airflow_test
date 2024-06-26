import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd

default_args = {
    'owner': 'UdeSA',
    'start_date': pendulum.datetime(2024, 3, 21, tz='UTC'),
}

# Este DAG consume principalmente la tabla "alumnos_programas", pero hace joins con muchas otras tablas (planes_grupos, planes_materias, alumnos_notas, etc)
# Su objetivo es generar como output (2 archivos CSVs en este caso):
# - data por alumnx, indicando cantidad de materias aprobadas y requeridas por cada año y semestre
# - data por alumnx, indicando cantidad de materias aprobadas y requeridas por cada carrera que cursa
#
# Los casos particulares que hay son:
# - cuando una persona cursa 2 carreras o más, se deben considerar las materias aprobadas de carreras diferentes a la que cursa actualmente
# - cuando una persona se cambió de carrera, se deben considerar las materias aprobadas de la carrera que ya no cursa

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
    AND an.m_aprueba_mat ='Si'
    ORDER BY pm."N_ANO_CARRERA", semestre
"""

query_planes_materias = """
    SELECT pm."C_IDENTIFICACION" , pm."C_PROGRAMA" , pm."C_ORIENTACION" , pm."C_PLAN", pm."N_ID_MATERIA"
    FROM planes_materias pm
"""

query_materias_carrera = """
    SELECT mpc.identificacion , mpc.programa , mpc.orientacion , mpc.plan, mpc.ano, mpc.semestre, mpc.cantidad_materias
    FROM materias_por_carrera mpc
"""

def get_data_with_queries():
    pg_hook = PostgresHook(postgres_conn_id="postgres_db")
    alumnos_programa = pg_hook.get_pandas_df(sql=query_alumnos_programas)
    planes_materias = pg_hook.get_pandas_df(sql=query_planes_materias)
    materias_carrera = pg_hook.get_pandas_df(sql=query_materias_carrera)
    planes_materias["unique_key"] = (planes_materias["C_IDENTIFICACION"].astype(int).astype(str) +
                                 planes_materias["C_PROGRAMA"].astype(int).astype(str) +
                                 planes_materias["C_ORIENTACION"].astype(int).astype(str) +
                                 planes_materias["C_PLAN"].astype(int).astype(str) +
                                 planes_materias["N_ID_MATERIA"].astype(int).astype(str)
                                ).astype(int)
    print("Queries ejecutados")
    return alumnos_programa,planes_materias,materias_carrera


def check_2_more_degrees(alumnos_programa, planes_materias, materias_carrera):
    counts = alumnos_programa[(alumnos_programa['C_BAJA'].isna())].groupby('N_ID_PERSONA')['N_ID_ALU_PROG'].transform('nunique')

    # obtengo las materias de las personas que cursan 2 carreras o más
    subjects_from_people_with_2_more_degrees = alumnos_programa[(alumnos_programa['C_BAJA'].isna()) & (counts >= 2)]
    alumnos_prog_with_2_more_degrees = subjects_from_people_with_2_more_degrees.drop_duplicates(subset=['N_ID_PERSONA','N_ID_ALU_PROG'])[['N_ID_PERSONA','N_ID_ALU_PROG', 'C_IDENTIFICACION', 'C_PROGRAMA', 'C_ORIENTACION', 'c_plan','cant_mat_requeridas']]

    # recorrer cada n_id_alu_prog y revisar si alguna de las materias de sus otras carreras le sirven para la actual
    alumnos_programa_to_add = []
    for _, registro in alumnos_prog_with_2_more_degrees.iterrows():
        id_persona = registro['N_ID_PERSONA']
        id_alu_prog = registro['N_ID_ALU_PROG']

        # ver si las materias de otra carrera estan en en planes_materias para la carrera que se está revisando
        subjects_from_other_degree = subjects_from_people_with_2_more_degrees[
            (subjects_from_people_with_2_more_degrees['N_ID_PERSONA'] == id_persona) & 
            (subjects_from_people_with_2_more_degrees['N_ID_ALU_PROG'] != id_alu_prog)
        ]
        for _, row in subjects_from_other_degree.iterrows():
            subject_id_to_check = row["N_ID_MATERIA"]
            key_to_find = int(f'{int(registro["C_IDENTIFICACION"])}{int(registro["C_PROGRAMA"])}{int(registro["C_ORIENTACION"])}{int(registro["c_plan"])}{int(subject_id_to_check)}')

            if not (planes_materias[planes_materias["unique_key"] == key_to_find]).empty:
                # busco la materia de la otra carrera en planes_materias de la carrera que estoy revisando
                # se "agrega" esa materia como aprobada en el plan actual (con el IPO actual para que figure en esta carrera tambien)
                filtered_materias = materias_carrera[
                        (materias_carrera['identificacion'] == registro["C_IDENTIFICACION"]) &
                        (materias_carrera['programa'] == registro["C_PROGRAMA"]) &
                        (materias_carrera['orientacion'] == registro["C_ORIENTACION"]) &
                        (materias_carrera['plan'] == registro["c_plan"]) &
                        (materias_carrera['ano'] == row["ano"]) &
                        (materias_carrera['semestre'] == row["semestre"])
                    ]
                cant_mat_req = filtered_materias['cantidad_materias'].values[0] if not filtered_materias.empty else 0

                alumno_data = {
                    "N_ID_PERSONA": row["N_ID_PERSONA"],
                    "N_ID_ALU_PROG": registro["N_ID_ALU_PROG"],
                    "N_PROMOCION": row["N_PROMOCION"],
                    "C_BAJA": row["C_BAJA"],
                    "F_BAJA": row["F_BAJA"],
                    "C_IDENTIFICACION": registro["C_IDENTIFICACION"],
                    "C_PROGRAMA": registro["C_PROGRAMA"],
                    "C_ORIENTACION": registro["C_ORIENTACION"],
                    "c_plan": registro["c_plan"],
                    "N_ID_MATERIA": row["N_ID_MATERIA"],
                    "ano": row["ano"],
                    "semestre": row["semestre"],
                    "cant_mat_requeridas": cant_mat_req
                }
                alumnos_programa_to_add.append(alumno_data)
    return pd.DataFrame(alumnos_programa_to_add)


def check_degree_changes(alumnos_programa, planes_materias):
    print("Chequeo de materias de personas que cambiaron de carrera")
    #obtengo las materias que son de una carrera en la cual la persona no está mas (se cambió de carrera)
    subjects_to_check = alumnos_programa[alumnos_programa['C_BAJA'] == "CC"]

    # obtengo la carrera actual para las personas que se cambiaron de carrera
    filtered_df = alumnos_programa[(alumnos_programa['C_BAJA'].isnull()) & (alumnos_programa["N_ID_PERSONA"].isin(subjects_to_check["N_ID_PERSONA"]))]
    # me quedo con una row por N_ID_ALU_PROG
    current_degree_for_people_with_cc = filtered_df.drop_duplicates(subset=["N_ID_ALU_PROG"])
    valid_subjects = []

    # reviso cuales de las materias de la carrera previa, deben ser consideradas para la carrera actual
    for _, row in subjects_to_check.iterrows():
        persona_row = current_degree_for_people_with_cc[current_degree_for_people_with_cc["N_ID_PERSONA"] == row["N_ID_PERSONA"]]
        if not persona_row.empty:
            current_data = persona_row[["C_IDENTIFICACION", "C_PROGRAMA", "C_ORIENTACION", "c_plan"]]
            subject_id_to_check = row["N_ID_MATERIA"]

            key_to_find = int(f'{int(current_data["C_IDENTIFICACION"].values[0])}{int(current_data["C_PROGRAMA"].values[0])}{int(current_data["C_ORIENTACION"].values[0])}{int(current_data["c_plan"].values[0])}{int(subject_id_to_check)}')

            # busco la materia de la carrera anterior en planes_materias de la carrera actual
            if not (planes_materias[planes_materias["unique_key"] == key_to_find]).empty:
                valid_subjects.append(row["N_ID_MATERIA"])
        else:
             print(f"No se encontraron materias de otra carrera que corresponden a la actual para la persona: {row['N_ID_PERSONA']} - N_ID_ALU_PROG: {row['N_ID_ALU_PROG']}")

    # quito de alumnos_programa las rows con las materias que no corresponden o son de una carrera dada de baja
    filtered_alumnos_programa = alumnos_programa[
        (
            (alumnos_programa["C_BAJA"] == "CC") & 
            (alumnos_programa["N_ID_MATERIA"].isin(valid_subjects))
         ) | 
         (alumnos_programa["F_BAJA"].isna())
    ]

    # para las personas que cambiaron de carrera (["C_BAJA"] == "CC"), se debe considerar el N_ID_ALU_PROG mas alto (pertence a la carrera actual)
    current_id_alu_prog_for_cc = filtered_alumnos_programa.groupby(["N_ID_PERSONA"])["N_ID_ALU_PROG"].max().reset_index(name='CURRENT_N_ID_ALU_PROG')
    merged_df = filtered_alumnos_programa.merge(current_id_alu_prog_for_cc, on='N_ID_PERSONA', how='left')
    merged_df.loc[merged_df["C_BAJA"] == "CC", 'N_ID_ALU_PROG'] = merged_df.loc[merged_df["C_BAJA"] == "CC", 'CURRENT_N_ID_ALU_PROG']
    merged_df.drop(columns=['CURRENT_N_ID_ALU_PROG'], inplace=True)

    return merged_df


def calculate_mat_req_and_aprob(filtered_alumnos_programa):
    # calculamos falta la suma de las materias aprobadas por alumno con detalle por ano y semestre
    grouped_alumnos_programa = filtered_alumnos_programa.groupby([
            "N_ID_PERSONA",
            "N_ID_ALU_PROG",
            "N_PROMOCION",
            "c_plan",
            "ano",
            "semestre",
            "cant_mat_requeridas"
        ]).size().reset_index(name='cant_mat_aprobadas')        
    grouped_alumnos_programa["estado"] = grouped_alumnos_programa["cant_mat_aprobadas"] - grouped_alumnos_programa["cant_mat_requeridas"]

        # calculamos falta la suma de las materias aprobadas por alumno (si una persona cursa 2 carreras, apareceran 2 rows)
    grouped_by_alumnos = grouped_alumnos_programa.groupby([
            "N_ID_PERSONA",
            "N_ID_ALU_PROG",
        ]).agg(
             tot_cant_mat_req=('cant_mat_requeridas', 'sum'),
             tot_cant_mat_apr=('cant_mat_aprobadas', 'sum')
        ).reset_index()
    grouped_by_alumnos["estado"] = grouped_by_alumnos["tot_cant_mat_apr"] - grouped_by_alumnos["tot_cant_mat_req"]
    return grouped_alumnos_programa,grouped_by_alumnos


def add_IPO_to_data(filtered_alumnos_programa, grouped_alumnos_programa, grouped_by_alumnos):
    print("Agregamos IPO a archivos")
    current_values = filtered_alumnos_programa.groupby(["N_ID_PERSONA", "N_ID_ALU_PROG"]).last().reset_index()
    grouped_alumnos_programa = grouped_alumnos_programa.merge(current_values[["N_ID_PERSONA", "N_ID_ALU_PROG", "C_IDENTIFICACION", "C_PROGRAMA", "C_ORIENTACION"]],
                                            on=["N_ID_PERSONA", "N_ID_ALU_PROG"], how="left")

    current_values = filtered_alumnos_programa.groupby(["N_ID_PERSONA", "N_ID_ALU_PROG"]).last().reset_index()
    grouped_by_alumnos = grouped_by_alumnos.merge(current_values[["N_ID_PERSONA", "N_ID_ALU_PROG", "C_IDENTIFICACION", "C_PROGRAMA", "C_ORIENTACION"]],
                                            on=["N_ID_PERSONA", "N_ID_ALU_PROG"], how="left")
                                        
    return grouped_alumnos_programa,grouped_by_alumnos


def process_data():
        alumnos_programa, planes_materias, materias_carrera = get_data_with_queries()

        # Chequeo de materias de personas que cursan 2 carreras o más
        alumnos_programa_to_add = check_2_more_degrees(alumnos_programa, planes_materias, materias_carrera)

        # Chequeo de materias de personas que cambiaron de carrera
        filtered_alumnos_programa = check_degree_changes(alumnos_programa, planes_materias)

        # agrego a materias de carreras CC y de personas con 2 carreras o más
        filtered_alumnos_programa = pd.concat([filtered_alumnos_programa, alumnos_programa_to_add], ignore_index=True)

        # elimino el campo c_baja y f_baja
        filtered_alumnos_programa = filtered_alumnos_programa.drop(columns=["C_BAJA", "F_BAJA"])

        print("Cálculo de materias aprobadas y agrupamiento por alumnx")
        grouped_alumnos_programa, grouped_by_alumnos = calculate_mat_req_and_aprob(filtered_alumnos_programa)

        grouped_alumnos_programa, grouped_by_alumnos = add_IPO_to_data(filtered_alumnos_programa, grouped_alumnos_programa, grouped_by_alumnos)

        # generamos archivos csv de la data necesaria para el dashboard
        grouped_alumnos_programa.to_csv("grouped_alumnos_programa.csv", index=False)
        grouped_by_alumnos.to_csv("grouped_by_alumnos.csv", index=False)



with DAG(
    dag_id="get_final_alumnos_programas",
    default_args=default_args,
    catchup=False,
    schedule_interval=None,  # Run manually,
    tags=['csv', 'postgres'],
) as dag:
    process_data_task = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

    # Dependencias entre tareas
    process_data_task