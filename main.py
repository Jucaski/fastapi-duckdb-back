from fastapi import Depends, FastAPI
from import_csv import create, show, columns, unique_columns
from services.clean_csv import clean_csv_in_chunks
from contextlib import contextmanager
from typing import Generator
from fastapi.encoders import jsonable_encoder
import duckdb

app = FastAPI()

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
async def get_unique_columns(col1: str, col2: str, con: DuckDBConn = Depends(get_db)):
    result = con.sql(f"SELECT DISTINCT {col1}, {col2} FROM deaths;").fetchall()
    return result