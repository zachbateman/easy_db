# easy_db

easy_db is a tool designed to quickly allow Python database interaction capabilities from a consolidated, simple user interface.

# Current Features

  - DataBase class can handle both SQLite and Access file-based databases
    - To "connect" to a database, use:
        ```sh
        db = easy_db.DataBase('test_sqlite3_db.db')
        ```
    - Then, retrieve table names with:
        ```sh
        db.pull_all_table_names()
        ```
    - Run a "SELECT * ..." query on any table:
        ```sh
        db.pull_full_table('TEST_TABLE')
        ```
        - returned object is a list of dicts where each dict represents a row and is form {column: value}
        - pull_full_table uses functools.lru_cache to limit repetative database queries

  - For more fine-grained control of database work, the`DataBase.provide_db_connection` method is a decorator that can provide functions with a database connection (and cursor if specified).
        ```sh
        db = easy_db.DataBase('test_sqlite3_db.db')

        @db.provide_db_connection(also_cursor=True)
        def awesome_function(conn, cursor, x):
            data = cursor.execute('...SPECIAL SQL...').fetchall()
            conn.close()
            return data
        ```

License
----
MIT
