import duckdb
import numpy

con = duckdb.connect("my_database.db")
def create():
    con.sql("CREATE TABLE deaths AS FROM read_csv_auto('db/test.csv');")
    con.sql("COPY deaths FROM 'db/test.csv'")
    con.close()

def show():
    tables = con.sql(f"SELECT * FROM deaths")
    result = tables.to_df().to_dict(orient="records")
    con.close()
    return result