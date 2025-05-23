import pytest
import duckdb
from fastapi.testclient import TestClient
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from main import app, get_db
from fastapi import HTTPException
import json

# Fixture for in-memory DuckDB database
@pytest.fixture
def in_memory_db():
    conn = duckdb.connect(":memory:")
    # Setup test data
    conn.execute("""
        CREATE TABLE RAWDATA (
            id INTEGER,
            name VARCHAR,
            cause VARCHAR,
            Anio VARCHAR,
            CVE_Grupo VARCHAR,
            Grupo VARCHAR,
            CVE_Enfermedad VARCHAR,
            CVE_Causa_def VARCHAR,
            Causa_def VARCHAR
        );
        INSERT INTO RAWDATA VALUES
            (1, 'John', 'Heart Disease', '2020', 'G1', 'Group1', 'E1', 'C1', 'Cause1'),
            (2, 'Jane', 'Cancer', '2021', 'G2', 'Group2', 'E1', 'C2', 'Cause2'),
            (3, 'Doe', 'Heart Disease', '2020', 'G3', 'Group3', 'E2', 'C3', 'Cause3');
    """)
    conn.execute("""
        CREATE TABLE ENFERMEDADES (
            CVE_Grupo VARCHAR,
            Grupo VARCHAR,
            CVE_Enfermedad VARCHAR,
            CVE_Causa_def VARCHAR,
            Causa_def VARCHAR
        );
        INSERT INTO ENFERMEDADES VALUES
            ('G1', 'Group1', 'E1', 'C1', 'Cause1'),
            ('G2', 'Group2', 'E1', 'C2', 'Cause2'),
            ('G3', 'Group3', 'E2', 'C3', 'Cause3');
    """)
    yield conn
    conn.close()

# Fixture to override get_db dependency
@pytest.fixture
def override_get_db(in_memory_db):
    def _get_db():
        return in_memory_db
    app.dependency_overrides[get_db] = _get_db
    yield
    app.dependency_overrides.clear()

# Fixture for TestClient
@pytest.fixture
def client(override_get_db):
    return TestClient(app)

# Test for / endpoint
def test_root(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Hello World"}

# Test for /create endpoint (mocking clean_csv_in_chunks)
# def test_create_table(client, in_memory_db, monkeypatch):
#     def mock_clean_csv_in_chunks(input_file, output_file):
#         # Simulate CSV cleaning by creating a dummy CSV
#         with open(output_file, 'w') as f:
#             f.write("id,name,cause\n1,John,Heart Disease\n")
    
#     monkeypatch.setattr("main.clean_csv_in_chunks", mock_clean_csv_in_chunks)
    
#     response = client.get("/create")
#     assert response.status_code == 200
#     assert response.json() == {"status": "Table created from Parquet"}
    
#     # Verify table exists
#     tables = in_memory_db.sql("SHOW TABLES").fetchall()
#     assert "deaths" in [row[0] for row in tables]

# Test for /show endpoint
def test_show_tables(client):
    response = client.get("/show/tables")
    assert response.status_code == 200
    data = response.json()
    assert "tables" in data
    assert any(table["name"] == "RAWDATA" for table in data["tables"])
    assert any(table["name"] == "ENFERMEDADES" for table in data["tables"])

# Test for /clean/column endpoint
def test_show_tables(client):
    response = client.get("/clean/columns_to_lower_case?table_name=RAWDATA")
    assert response.status_code == 200
    data = response.json()
    assert "columns" in data
    assert set(data["columns"]) == {"id", "name", "cause", "anio", "cve_grupo", "grupo", "cve_enfermedad", "cve_causa_def", "causa_def"}
    assert set(data["columns"]) == {"Id", "Name", "Cause", "Anio", "CVE_Grupo", "Grupo", "CVE_Enfermedad", "CVE_Causa_def", "Causa_def"}

# Test for /columns endpoint
def test_get_columns(client):
    response = client.get("/show/columns?table_name=RAWDATA")
    assert response.status_code == 200
    data = response.json()
    assert "columns" in data
    assert set(data["columns"]) == {"id", "name", "cause", "Anio", "CVE_Grupo", "Grupo", "CVE_Enfermedad", "CVE_Causa_def", "Causa_def"}

# Test for /unique_columns endpoint
def test_get_unique_columns(client):
    response = client.get("/unique_pair_columns?column1=name&column2=cause&table=RAWDATA")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    assert ["John", "Heart Disease"] in data

# Test for /get_second_class endpoint
def test_get_second_class(client):
    response = client.get("/get_second_level_class?id_first_level_class=E1&ordered_by=Grupo")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert ["G1", "Group1"] in data
    assert ["G2", "Group2"] in data

# Test for /get_third_class endpoint
def test_get_third_class(client):
    response = client.get("/get_third_level_class?id_first_level_class=E1&id_second_class=G1&ordered_by=Causa_def")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert ["C1", "Cause1"] in data

# Test for /get_unique endpoint
def test_get_unique_values(client):
    response = client.get("/unique_values_by_column?column_name=cause&table=RAWDATA")
    assert response.status_code == 200
    data = response.json()
    assert ["Heart Disease"] in data
    assert ["Cancer"] in data

# Test for /get_unique with invalid column
def test_get_unique_values_invalid_column(client):
    response = client.get("/unique_values_by_column?column_name=invalid&table=ENFERMEDADES")
    assert response.status_code == 500
    assert "not found" in response.json()["detail"]

# Test for /columns with invalid table
def test_get_columns_invalid_table(client, in_memory_db):
    # Simulate a missing table
    in_memory_db.execute("DROP TABLE RAWDATA")
    response = client.get("/show/columns?table_name=RAWDATA")
    assert response.status_code == 500
    assert "Query error" in response.json()["detail"]

# Test for /unique_columns with invalid column
def test_get_unique_columns_invalid_column(client):
    response = client.get("/unique_pair_columns?column1=invalid&column2=cause&table=ENFERMEDADES")
    assert response.status_code == 500
    assert "not found" in response.json()["detail"]

# Test for /get_second_class with no data
def test_get_second_class_no_data(client):
    response = client.get("/get_second_level_class?id_first_level_class=invalid&ordered_by=ENFERMEDADES")
    assert response.status_code == 200
    assert response.json() == {"message": "No data found"}

# Test for /get_second_class with missing parameter
def test_get_second_class_missing_param(client):
    response = client.get("/get_second_level_class")
    assert response.status_code == 422
    assert any(error["type"] == "missing" for error in response.json()["detail"])

# Test for /get_third_class with no data
def test_get_third_class_no_data(client):
    response = client.get("/get_third_level_class?id_first_level_class=E1&id_second_class=invalid&ordered_by=ENFERMEDADES")
    assert response.status_code == 200
    assert response.json() == {"message": "No data found"}

# Test for /get_third_class with missing parameters
def test_get_third_class_missing_params(client):
    response = client.get("/get_third_level_class?id_first_level_class=E1")
    assert response.status_code == 422
    assert any(error["type"] == "missing" for error in response.json()["detail"])