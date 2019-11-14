'''
Utility functions for easy_db.
'''
import os
import sqlite3, pyodbc
from typing import List, Dict, Any



def check_if_file_is_sqlite(filename: str) -> bool:
    '''
    Check if file is a sqlite database.
    See:  https://stackoverflow.com/questions/12932607/how-to-check-if-a-sqlite3-database-exists-in-python
    '''
    if not os.path.isfile(filename):
        return False

    if os.path.getsize(filename) < 100:  # SQLite db file header is 100 bytes (minimum file size)
        return False

    with open(filename, 'rb') as possible_db_file:
        header = possible_db_file.read(100)

    if header[:16] == b'SQLite format 3\x00':
        return True
    else:
        return False


def list_of_dicts_from_query(cursor, sql: str, tablename: str, db_type: str, parameters: list=[]) -> List[Dict[str, Any]]:
    '''
    Query db using cursor, supplied sql, and tablename.
    Return list of dicts for query result.
    '''
    try:
        data = cursor.execute(sql, parameters).fetchall()
    except (sqlite3.OperationalError, pyodbc.ProgrammingError) as error:
        print(f'ERROR querying table {tablename}!  Error below:')
        print(error)
        print(f'SQL: {sql}')
        return

    if db_type == 'SQLITE3':
        columns = [description[0] for description in cursor.description]
    elif db_type == 'SQL SERVER':
        columns = [column[0] for column in cursor.description]
    else:
        try:
            columns = [row.column_name for row in cursor.columns(table=tablename)]
        except UnicodeDecodeError:
            print('\nERROR - Unable to read column names.')
            print('This may occur if using Access database with column descriptions populated.')
            print('Try deleting the column descriptions.\n')
    table_data = [dict(zip(columns, row)) for row in data]
    return table_data


# set for quickly checking possibly malicious characters
unallowed_characters = {';', '(', ')', '-', '=', '+', "'", '"', '.', '[', ']', ',',
    '{', '}', '\\', '/', '`', '~', '!', '@', '#', '$', '%', '^', '&', '*'}

def name_clean(name: str) -> bool:
    '''
    Check name and return True if it looks clean (not malicious).
    Return False if it name could be attempting sql injection.

    Used for table names and column names (as these can't be parameterized).
    '''
    for char in name:
        if char in unallowed_characters:
            print(f'ERROR!!!  Prohibited characters detected in:\n  {name}')
            return False
    if 'DROP' in name.upper():
        print(f'ERROR!!!  Prohibited characters detected in:\n  {name}')
        return False
    return True
