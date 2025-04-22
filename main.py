from fastapi import FastAPI
from import_csv import create, show, columns, unique_columns

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/create")
async def lists():
    return {create()}

@app.get("/show")
async def lists2():
    return show()

@app.get("/columns")
async def get_columns():
    result = columns()
    return result

@app.get("/unique_columns")
async def get_unique_columns(col1: str, col2: str):
    result = unique_columns(col1, col2)
    return result