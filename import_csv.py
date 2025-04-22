import duckdb
import numpy
from fastapi.encoders import jsonable_encoder

con = duckdb.connect("my_database.db")

def create():
    con.sql("CREATE TABLE  AS FROM read_csv_auto('db/test.csv');")
    con.sql("COPY deaths FROM 'db/test.csv'")
    con.close()

def show():
    tables = con.sql("SELECT * FROM deaths")
    result = tables.to_df().to_dict(orient="records")
    con.close()
    return result

def columns():
    rel = con.sql(f"DESCRIBE deaths")
    column_names = [row[0] for row in rel.fetchall()] 
    con.close()
    return jsonable_encoder({"columns": column_names})

def unique_columns(col1: str, col2:str):
    rel = con.sql(f"SELECT DISTINCT {col1}, {col2} FROM deaths;").fetchall()
    con.close()
    return rel