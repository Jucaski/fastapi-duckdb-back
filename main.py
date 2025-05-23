from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import contextmanager
from typing import Generator
from fastapi.encoders import jsonable_encoder
from services.clean_csv import clean_csv_in_chunks
import os

from os import listdir
from os.path import isfile, join
import duckdb

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DuckDBConn = duckdb.DuckDBPyConnection
db_connection = None

@contextmanager
def get_db_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    con = duckdb.connect("db/my_database.db")
    try:
        db_connection.sql("SET threads TO 8;")
        db_connection.sql("SET memory_limit = '8GB';")
        db_connection.sql("PRAGMA enable_parallelism;")
        yield con
    finally:
        con.close()

def get_db() -> duckdb.DuckDBPyConnection:
    yield db_connection

def init_db():
    try:
        if not os.path.exists('db/cleaned_file.csv'):
            clean_csv_in_chunks('db/def00_19_v2.csv', 'db/cleaned_file.csv')
        db_connection.sql("""
            COPY (SELECT * FROM read_csv_auto('db/cleaned_file.csv', auto_detect=true, header=true))
            TO 'db/RAWDATA.parquet' (FORMAT PARQUET);
        """)
        db_connection.sql("""
            CREATE OR REPLACE TABLE RAWDATA AS
            SELECT * FROM 'db/RAWDATA.parquet';
        """)
        db_connection.sql("""
            CREATE OR REPLACE TABLE ENFERMEDADES AS
            SELECT DISTINCT CVE_Grupo, Grupo, CVE_Enfermedad, CVE_Causa_def, Causa_def
            FROM RAWDATA;
        """)
        db_connection.sql("CREATE INDEX IF NOT EXISTS id_enfermedad ON ENFERMEDADES (CVE_Enfermedad, CVE_Grupo, CVE_Causa_def);")
        db_connection.sql("""
            CREATE OR REPLACE TABLE DEFUNCIONES AS
            SELECT CVE_Enfermedad, CVE_Grupo, CVE_Causa_def, CVE_Estado,
            CVEGEO, CVE_Metropoli, Ambito, Sexo, Edad_gpo, Ocupacion, Escolaridad, Edo_civil, Anio
            FROM RAWDATA;
        """)
        db_connection.sql("""
            CREATE OR REPLACE TABLE ESTADO_MUN AS
            SELECT DISTINCT CVE_Estado, Estado, CVEGEO, Municipio
            FROM RAWDATA;
        """)
        db_connection.sql("""
            CREATE OR REPLACE TABLE METROPOLI AS
            SELECT DISTINCT CVE_Metropoli, Metropolis
            FROM RAWDATA;
        """)
    except Exception as e:
        print(f"Error creating table: {e}")
        tables = db_connection.sql("SHOW TABLES").fetchall()
        print("Available tables:", [row[0] for row in tables])

@app.on_event("startup")
async def startup_event():
    global db_connection
    db_connection = duckdb.connect("db/my_database.db")
    db_connection.sql("SET threads TO 4;")
    db_connection.sql("SET memory_limit = '4GB';")
    #init_db()

@app.on_event("shutdown")
async def shutdown_event():
    global db_connection
    if db_connection:
        db_connection.close()

@app.get("/")
async def root(con: DuckDBConn = Depends(get_db)):
    return {"message": "Hello World"}

@app.get("/create")
async def create_table(con: DuckDBConn = Depends(get_db)):
    try:
        first_chunk = True
        db_dir = "db"
        name_of_cleaned_file = "cleaned_table_file.csv"
        csv_cleaned_name = join(db_dir, name_of_cleaned_file)
        for file in listdir("db/"):
            #First, we check for .csv files that area not cleaned
            if file.endswith(".csv") and not file.startswith("cleaned_"):
                csv_file_dir = join(db_dir, file)
                #If the file cleaned_file does not exist, we clean and create it
                if not isfile(csv_cleaned_name):
                    clean_csv_in_chunks(first_chunk, csv_file_dir, csv_cleaned_name)
                #We create the table
                con.sql(f"""
                        CREATE OR REPLACE TABLE deaths AS
                        SELECT * FROM read_csv_auto('{csv_cleaned_name}',
                        auto_detect=true, header=true);""")
                con.sql(f"""
                        COPY (SELECT * FROM read_csv_auto('{csv_cleaned_name}', auto_detect=true, header=true))
                        TO 'db/deaths.parquet' (FORMAT PARQUET);""")
                con.sql("""CREATE OR REPLACE TABLE deaths AS SELECT * FROM 'db/deaths.parquet';""")
                #Finally, we lower all column names for ease of access
                #table_name = "deaths"
                #columns = con.sql(f"SELECT * FROM {table_name}").columns
                #for column in columns:
                 #   if isinstance(column, str):
                  #      temp_column_name = column
                   #     column_name_lower = temp_column_name.lower()
                    #    if (temp_column_name != column_name_lower):
                     #       con.sql(f"""ALTER TABLE {table_name} RENAME COLUMN {temp_column_name} TO 
                      #              {column_name_lower}""")
                if first_chunk:
                    first_chunk = False
                con.close()
                return {"status": "Table created from Parquet"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating table: {str(e)}")

@app.get("/clean/columns_to_lower_case")
async def columns_to_lower_case(table_name: str, con: DuckDBConn = Depends(get_db)):
    try:
        columns_to_lower = con.sql(f"SELECT * FROM {table_name}").columns
        for column in columns_to_lower:
            if isinstance(column, str):
                temp_column_name = column
                column_name_lower = temp_column_name.lower()
                if (temp_column_name != column_name_lower):
                    con.sql(f"""ALTER TABLE {table_name} RENAME COLUMN {temp_column_name} TO 
                            {column_name_lower}""")
        result = con.sql(f"SELECT * FROM {table_name}").columns
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

#@app.get("/clean/clean_encoding_csv")
#async def clean_csv(List[str], con: DuckDBConn = Depends(get_db)):
 #   try:

@app.get("/show/tables")
async def lists2(con: DuckDBConn = Depends(get_db)):
    try:
        tables = con.sql("SHOW TABLES")
        result = tables.to_df().to_dict(orient="records")
        return {"tables": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/show/columns")
async def get_columns(table_name:str, con: DuckDBConn = Depends(get_db)):
    try:
        rel = con.sql(f"DESCRIBE {table_name}")
        column_names = [row[0] for row in rel.fetchall()]
        return jsonable_encoder({"columns": column_names})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/unique_pair_columns")
async def get_unique_columns(column1: str, column2: str, table: str, con: DuckDBConn = Depends(get_db)):
    try:
        result = con.sql(f"SELECT DISTINCT {column1}, {column2} FROM {table};").fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


@app.get("/get_second_level_class")
async def get_second_class_list(id_first_level_class: str, ordered_by:str, con: DuckDBConn = Depends(get_db)):
    try:
        if not id_first_level_class or not ordered_by:
            raise HTTPException(status_code=400, detail="Invalid input: id_first_level_class and ordered_by are required")
        result = con.sql("""
            SELECT DISTINCT CVE_Grupo, Grupo
            FROM ENFERMEDADES
            WHERE CVE_Enfermedad = ?
            ORDER BY ?
        """, params=[id_first_level_class, ordered_by]).fetchall()
        return result if result else {"message": "No data found"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/get_third_level_class")
async def get_third_class_list(id_first_level_class: str, id_second_class: str, ordered_by:str, con: DuckDBConn = Depends(get_db)):
    try:
        if not id_sick or not id_second_class:
            raise HTTPException(status_code=400, detail="Invalid input: id_first_level_class, id_second_class and ordered_by are required")
        result = con.sql("""
            SELECT DISTINCT cve_causa_def, causa_def
            FROM ENFERMEDADES
            WHERE cve_grupo = ? AND cve_enfermedad = ?
            ORDER BY ?
        """, params=[id_second_class, id_first_level_class, ordered_by]).fetchall()
        return result if result else {"message": "No data found"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/records_by_year_by_column")
async def get_records_year(year: str, table:str, con: DuckDBConn = Depends(get_db)):
    try:
        if not id_sick or not id_second_class:
            raise HTTPException(status_code=400, detail="Invalid input: year and table are required")
        result = con.sql(f"SELECT * FROM {table} WHERE Anio={year};").fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/unique_values_by_column")
async def get_unique_values(column_name: str, table:str, con: DuckDBConn = Depends(get_db)):
    try:
        if not column_name or not table:
            raise HTTPException(status_code=400, detail="Invalid input: column_name and table are required")
        result = con.sql(f"SELECT DISTINCT {column_name} FROM {table} ORDER BY {column_name}").fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/get_all_by_table")
async def get_unique_values(table:str, con: DuckDBConn = Depends(get_db)):
    try:
        if not table:
            raise HTTPException(status_code=400, detail="Invalid input: column_name and table are required")
        result = con.sql(f"SELECT * FROM {table}").fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")