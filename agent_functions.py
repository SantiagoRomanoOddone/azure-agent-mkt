import pandas as pd
import sqlite3
from typing import Any, Callable, Set


# schema functions
def get_table_schema(table: str) -> Any:
    """
    Returns the schema (column names and data types) of the contractual table
    """
    if table == 'contractual':
        df = pd.read_csv("data/contractual_dummy_data.csv")
        schema = [{"column": col, "dtype": str(df[col].dtype)} for col in df.columns]
    elif table == 'earned':
        df = pd.read_csv("data/earned_dummy_data.csv")
        schema = [{"column": col, "dtype": str(df[col].dtype)} for col in df.columns]
    else:
        return f"Table '{table}' not found."

    return schema



def execute_sql(query: str, table: str) -> Any:
    """
    Executes a SQL query on the specified table's CSV data.
    The table is loaded into an in-memory SQLite database for querying.
    """
    if table == "contractual":
        df = pd.read_csv("data/contractual_dummy_data.csv")
    elif table == "earned":
        df = pd.read_csv("data/earned_dummy_data.csv")
    else:
        return f"Table '{table}' not found."

    # Create in-memory SQLite DB and register it under the real table name
    conn = sqlite3.connect(":memory:")
    df.to_sql(table, conn, index=False, if_exists="replace")

    # Execute the query
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()

    conn.close()
    return results


# Register this single function so the agent can call it
agent_functions: Set[Callable[..., Any]] = {
    execute_sql,
    get_table_schema
}
