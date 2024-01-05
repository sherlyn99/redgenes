import sqlite3

def connect_to_database(db_connection):
    """
    Connect to the SQLite database and return the connection and cursor objects.
    """
    if isinstance(db_connection, str):
        conn = sqlite3.connect(db_connection)
    elif isinstance(db_connection, sqlite3.Connection):
        conn = db_connection
    else:
        raise TypeError("db_connection must be a file path (str) or a sqlite3.Connection object.")
    
    cursor = conn.cursor()
    return conn, cursor

def close_database_connection(conn):
    """
    Close the SQLite database connection.
    """
    conn.close()

def subset_isolates_by_metadata(cursor, subset, column):
    """
    Retrieve all isolates that fit a metadata category.
    """
    try:
        # Execute a query to fetch all entries from the specified column in the table
        cursor.execute(f"SELECT filename, filepath FROM metadata WHERE {column} = ?", (subset,))
        # Fetch the results
        entries = cursor.fetchall()
        return entries
    except sqlite3.OperationalError as e:ß
        print(f"Error: {e}")
        return None  # or return an appropriate value or raise an exception