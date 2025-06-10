
import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime

# Configuración de conexión
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'ingicat_db',
    'user': 'fabian',
    'password': 'ingicat*1003'
}

# Ruta al archivo Excel
EXCEL_PATH = '20250404_BASE DOCUMENTAL_PREDIOS PROPIOS.xlsx'
SHEET_NAME = 'T_DOCUM'
TABLE_NAME = 'task_predio'  # tabla de tu modelo Predio

# Leer archivo Excel
try:
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, header=1)  
    print("Columnas disponibles en el Excel:", df.columns.tolist())
    print(f"Archivo leído correctamente. Filas: {len(df)}")
except Exception as e:
    print(f"Error leyendo el archivo Excel: {e}")
    exit(1)

# Reemplazar NaN por None
df = df.where(pd.notnull(df), None)

# Normalizar nombres de columnas para que coincidan con los del modelo
# Aquí mapea nombres del Excel → campos del modelo Django
column_mapping = {
    'PROYECTO': 'proyecto',
    'VIGENCIA': 'vigencia',
    'GERENCIA': 'gerencia',
    'CATEGORIA_PREDIO_FMI': 'categoria_predio_fmi',
    'ESTADO_FOLIO_MATRICULA': 'estado_folio_matricula',
    'CATEGORIA_FMI': 'categoria_fmi',
    'TIPO_DOCUMENTAL': 'tipo_documental',
    'ESTADO': 'estado',
    'ESTADO_COMPRA': 'estado_compra',
    'SUB_ESTADO_COMPRA': 'sub_estado_compra',
    'ENVIO_OPEN_TEXT': 'envio_open_text',
    'ACCION_TECNICA': 'accion_tecnica',
    'FECHA_SOLICITUD': 'fecha_solicitud',
    'FECHA_RESPUESTA': 'fecha_respuesta',
    'DATECOMPLETED': 'datecompleted',
    'ES_IMPORTANTE': 'es_importante',
    'FECHA_REITERACION': 'fecha_reiteracion',
    'ULTIMA_FECHA_ACCESO': 'ultima_fecha_acceso',
    'CAMPO': 'campo',
    'COD_SIG': 'cod_sig',
    'FMI': 'fmi',
    'CED_CATASTRAL': 'ced_catastral',
    'NOM_PREDIO': 'nom_predio',
    'DOCUMENTO': 'documento',
    'FECHA_DOCUMENTO': 'fecha_documento',
    'ENTIDAD': 'entidad',
    'MUNICIPIO': 'municipio',
    'DOCUMENTOS_MUNICIPIO': 'documentos_municipio',
    'NOMBRE_PREDIO_OPENTEXT': 'nombre_predio_opentext',
    'COD_SIG_OPENTEXT': 'cod_sig_opentext',
    'COD_SIG_ASOCIADO': 'cod_sig_asociado',
    'FECHA_PAGO': 'fecha_pago',
    'VALOR_PAGO': 'valor_pago',
    'FECHA_ADQUISICION': 'fecha_adquisicion',
    'ESTRATEGIA': 'estrategia',
    'RESPONSABLE_ADQUISICION': 'responsable_adquisicion',
    'LINK_SHAREPOINT': 'link_sharepoint',
    'RESPONSABLE_SEGUIMIENTO': 'responsable_seguimiento',
    'FECHA_NUEVA_BUSQUEDA': 'fecha_nueva_busqueda',
    'RESPONSABLE_NUEVA_BUSQUEDA': 'responsable_nueva_busqueda',
    'COD_ESPECIFICACION': 'cod_especificacion',
    'ADQUIRIR': 'adquirir',
    'REPETIDO': 'repetido',
    'PAQUETE': 'paquete'
}

# Renombrar columnas según el mapeo
df.rename(columns=column_mapping, inplace=True)

# Seleccionar solo las columnas que existen en el modelo
model_fields = list(column_mapping.values())
df = df[model_fields]

# Convertir columnas de fecha explícitamente
date_fields = [
    'fecha_solicitud', 'fecha_respuesta', 'datecompleted', 'fecha_reiteracion',
    'ultima_fecha_acceso', 'fecha_documento', 'fecha_pago', 'fecha_adquisicion',
    'fecha_nueva_busqueda'
]
for field in date_fields:
    if field in df.columns:
        df[field] = pd.to_datetime(df[field], errors='coerce').dt.date

# Convertir booleanos si aplica
boolean_fields = ['es_importante', 'adquirir', 'repetido']
for field in boolean_fields:
    if field in df.columns:
        df[field] = df[field].map(lambda x: bool(x) if x is not None else None)

# Preparar consulta SQL dinámica
insert_query = sql.SQL("""
    INSERT INTO {table} ({fields})
    VALUES ({placeholders})
""").format(
    table=sql.Identifier(TABLE_NAME),
    fields=sql.SQL(', ').join(map(sql.Identifier, df.columns)),
    placeholders=sql.SQL(', ').join(sql.Placeholder() * len(df.columns))
)

# Conectar a la base de datos
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("Conexión a la base de datos establecida.")
except Exception as e:
    print(f"Error conectando a la base de datos: {e}")
    exit(1)

# Insertar fila por fila
errores = 0

for idx, row in enumerate(df.itertuples(index=False, name=None), start=1):
    try:
        cur.execute(insert_query, row)
    except Exception as e:
        errores += 1
        print(f"[Error fila {idx}] {e}")
        conn.rollback()
    else:
        conn.commit()

# Cerrar conexión
cur.close()
conn.close()
print(f"Carga finalizada. Filas con error: {errores}")
