from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import contextmanager
from typing import Generator
from fastapi.encoders import jsonable_encoder
from services.clean_csv import clean_csv_in_chunks
from os import listdir
from os.path import isfile, join, splitext
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
        con.sql("SET threads TO 4;")
        con.sql("SET memory_limit = '4GB';")
        yield con
    finally:
        con.close()

def get_db() -> duckdb.DuckDBPyConnection:
    yield db_connection

def init_db():
    try:
        db_connection.sql("""
            CREATE OR REPLACE TABLE death_cause_agg AS
            SELECT DISTINCT CVE_Grupo, Grupo, CVE_Enfermedad, CVE_Causa_def, Causa_def
            FROM deaths;
        """)
        db_connection.sql("CREATE INDEX IF NOT EXISTS idx_group_sick ON death_cause_agg (CVE_Grupo, CVE_Enfermedad);")
        print("Table 'death_cause_agg' and index created successfully.")
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
    init_db()

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
        db_dir = "db"
        for file in listdir("db/"):
            #First, we check for .csv files that area not cleaned
            if file.endswith(".csv") and not file.startswith("cleaned_"):
                csv_file_dir = join(db_dir, file)
                name_of_cleaned_file = "cleaned_" + file
                csv_cleaned_name = join(db_dir, name_of_cleaned_file)
                #If the file cleaned_file does not exist, we clean and create it
                if not isfile(csv_cleaned_name):
                    clean_csv_in_chunks(csv_file_dir, csv_cleaned_name)
                #table_name = splitext(file)[0] //Check later table names on another issue
                #We create the table (for now with an static name)
                con.sql(f"""
                        CREATE OR REPLACE TABLE deaths AS
                        SELECT * FROM read_csv_auto('{csv_cleaned_name}',
                        auto_detect=true, header=true);""")
                con.sql(f"""
                        COPY (SELECT * FROM read_csv_auto('{csv_cleaned_name}', auto_detect=true, header=true))
                        TO 'db/deaths.parquet' (FORMAT PARQUET);""")
                con.sql("""CREATE OR REPLACE TABLE deaths AS SELECT * FROM 'db/deaths.parquet';""")
                #Finally, we lower all column names for ease of access
                table_name = "deaths"
                columns = con.sql(f"SELECT * FROM {table_name}").columns
                for column in columns:
                    if isinstance(column, str):
                        temp_column_name = column
                        column_name_lower = temp_column_name.lower()
                        if (temp_column_name != column_name_lower):
                            con.sql(f"""ALTER TABLE {table_name} RENAME COLUMN {temp_column_name} TO 
                                    {column_name_lower}""")
                con.close()
                return {"status": "Table created from Parquet"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating table: {str(e)}")

@app.get("/show")
async def lists2(con: DuckDBConn = Depends(get_db)):
    tables = con.sql("SHOW TABLES")
    result = tables.to_df().to_dict(orient="records")
    return {"tables": result}

@app.get("/columns")
async def get_columns(con: DuckDBConn = Depends(get_db)):
    rel = con.sql("DESCRIBE deaths")
    column_names = [row[0] for row in rel.fetchall()]
    return jsonable_encoder({"columns": column_names})

@app.get("/unique_columns")
async def get_unique_columns(column1: str, column2: str, con: DuckDBConn = Depends(get_db)):
    try:
        result = con.sql(f"SELECT DISTINCT {column1}, {column2} FROM deaths;").fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


@app.get("/get_second_class")
async def get_second_class_list(id_sick: str, con: DuckDBConn = Depends(get_db)):
    try:
        if not id_sick:
            raise HTTPException(status_code=400, detail="Invalid input: id_sick are required")
        result = con.sql("""
            SELECT DISTINCT cve_grupo, grupo
            FROM death_cause_agg
            WHERE cve_enfermedad = ?
            ORDER BY grupo
        """, params=[id_sick]).fetchall()
        return result if result else {"message": "No data found"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/get_third_class")
async def get_third_class_list(id_sick: str, id_second_class: str, con: DuckDBConn = Depends(get_db)):
    try:
        if not id_sick or not id_second_class:
            raise HTTPException(status_code=400, detail="Invalid input: id_sick and id_second_class are required")
        result = con.sql("""
            SELECT DISTINCT cve_causa_def, causa_def
            FROM death_cause_agg
            WHERE cve_grupo = ? AND cve_enfermedad = ?
            ORDER BY causa_def
        """, params=[id_second_class, id_sick]).fetchall()
        return result if result else {"message": "No data found"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

# @app.get("/get_records_year")
# async def get_records_year(year: str, con: DuckDBConn = Depends(get_db)):
#     result = con.sql(f"SELECT * FROM deaths WHERE anio={year};").fetchall()
#     return result

@app.get("/get_unique")
async def get_unique_values(column_name: str, con: DuckDBConn = Depends(get_db)):
    result = con.sql(f"SELECT DISTINCT {column_name} FROM deaths ORDER BY {column_name}").fetchall()
    return result