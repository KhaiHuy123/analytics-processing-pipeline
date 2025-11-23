
import duckdb
import os
import pytest

def connect_to_motherduck(connection_string):
    conn = duckdb.connect(database=connection_string, read_only=False)
    return conn

def test_connect_to_motherduck():
    database = os.getenv("MOTHER_DUCK_DATABASE")
    token = os.getenv("MOTHER_DUCK_TOKEN")
    try:
        connection_string = f"md:{database}?motherduck_token={token}"
        conn = connect_to_motherduck(connection_string)
        assert conn is not None, "Connection object is None"
    except Exception as e:
        pytest.fail(f"Connection to MotherDuck failed: {e}")
