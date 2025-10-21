import pandas as pd
import sqlite3
from typing import Any, Callable, Set

def execute_sql(query: str) -> Any:
    """
    Executes a SQL query on the data loaded from data/table1.csv.
    The table is loaded into an in-memory SQLite database for querying.
    """
    # Load the CSV
    df = pd.read_csv("data/table1.csv")

    # Create in-memory SQLite DB
    conn = sqlite3.connect(":memory:")
    df.to_sql("table1", conn, index=False, if_exists="replace")

    # Execute the query
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()

    conn.close()
    return results

# Register this single function so the agent can call it
agent_functions: Set[Callable[..., Any]] = {
    execute_sql,
}
