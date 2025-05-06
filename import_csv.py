import duckdb
import numpy
from fastapi.encoders import jsonable_encoder
import csv
from services.clean_csv import clean_csv_in_chunks
from os import listdir
from os.path import isfile, join


def create():
    csv_file_dir = ""
    csv_cleaned_name = ""
    files_in_db_directory = []
    for file in listdir('db/'):
        if file.endswith(".csv"):
            csv_file_dir = join('db/', file)
            csv_cleaned_name = join('db/cleaned_', file)
            files_in_db_directory.append(file)
            clean_csv_in_chunks(csv_file_dir, csv_cleaned_name)
            con.sql(f"""CREATE OR REPLACE TABLE deaths AS
                    SELECT * FROM read_csv_auto({csv_cleaned_name},
                    auto_detect=true, header=true);""")
            #con.sql("COPY deaths FROM 'db/test.csv'")
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