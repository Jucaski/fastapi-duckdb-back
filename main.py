from fastapi import FastAPI
from import_csv import create, show

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/create")
async def lists():
    return {create()}

@app.get("/show")
async def lists():
    return show()