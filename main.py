from fastapi import Depends, FastAPI
from import_csv import create, show, columns, unique_columns
from services.clean_csv import clean_csv_in_chunks
from contextlib import contextmanager
from typing import Generator
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
import duckdb

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],  # Allow your frontend origin
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

DuckDBConn = duckdb.DuckDBPyConnection

@contextmanager
def get_db_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    con = duckdb.connect("db/my_database.db")
    try:
        yield con
    finally:
        con.close()

def get_db() -> duckdb.DuckDBPyConnection:
    with get_db_connection() as con:
        yield con

def init_db():
    with get_db_connection() as con:
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS death_cause AS
                SELECT CVE_Grupo, CVE_Causa_def, Causa_def
                FROM deaths
            """)
            print("Table 'death_cause' created successfully.")
        except Exception as e:
            print(f"Error creating table: {e}")
            tables = con.sql("SHOW TABLES").fetchall()
            print("Available tables:", [row[0] for row in tables])

@app.on_event("startup")
async def startup_event():
    init_db()


@app.get("/")
async def root(con: DuckDBConn = Depends(get_db)):
    return {"message": "Hello World"}

@app.get("/create")
async def lists(con: DuckDBConn = Depends(get_db)):
    clean_csv_in_chunks('db/def00_19_v2.csv', 'db/cleaned_file.csv')
    con.sql(f"""CREATE OR REPLACE TABLE deaths AS
            SELECT * FROM read_csv_auto('db/cleaned_file.csv',
                    auto_detect=true,
                    header=true);""")
    #con.sql("COPY deaths FROM 'db/test.csv'")
    return {"status": "Table created"}

@app.get("/show")
async def lists2(con: DuckDBConn = Depends(get_db)):
    tables = con.sql("SHOW TABLES")
    result = tables.to_df().to_dict(orient="records")
    return {"tables": result}

@app.get("/columns")
async def get_columns(con: DuckDBConn = Depends(get_db)):
    rel = con.sql(f"DESCRIBE deaths")
    column_names = [row[0] for row in rel.fetchall()] 
    return jsonable_encoder({"columns": column_names})

@app.get("/unique_columns")
async def get_unique_columns(column1: str, column2: str, con: DuckDBConn = Depends(get_db)):
    result = con.sql(f"SELECT DISTINCT {column1}, {column2} FROM deaths;").fetchall()
    return result

@app.get("/get_third_class")
async def get_unique_columns(id_second_class: str, con: DuckDBConn = Depends(get_db)):
    result = con.sql(f"SELECT * FROM death_cause WHERE CVE_Grupo={id_second_class};").fetchall()
    return result
