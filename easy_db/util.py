'''
Utility functions for easy_db.
'''
import os
import sqlite3, pyodbc
from typing import List, Dict, Any
from datetime import datetime



def type_map(db_type) -> dict:
    '''
    Return dict of Python types as keys and appropriate
    database types as values based on the provided db_type.
    '''
    if db_type == 'ACCESS':
        return {float: 'double',
                    'float': 'double',
                    'double': 'double',
                    'float64': 'double',
                    'numpy.float64': 'double',
                    int: 'integer',
                    'int': 'integer',
                    'integer': 'integer',
                    str: 'varchar(255)',
                    'str': 'varchar(255)',
                    'text': 'varchar(255)',
                    'varchar': 'varchar(255)',
                    'date': 'datetime',
                    'datetime': 'datetime',
                    datetime: 'datetime',
                    'timestamp': 'datetime',
                    'smallint': 'integer',
                    None: 'varchar(255)',
                    'nonetype': 'varchar(255)',
                    }
    elif db_type == 'SQLITE':
        return {float: 'REAL',
                    'float': 'REAL',
                    'double': 'REAL',
                    'real': 'REAL',
                    'float64': 'REAL',
                    'numpy.float64': 'REAL',
                    int: 'INTEGER',
                    'int': 'INTEGER',
                    'integer': 'INTEGER',
                    str: 'TEXT',
                    'str': 'TEXT',
                    'text': 'TEXT',
                    'varchar': 'TEXT',
                    'date': 'DATE',
                    'datetime': 'TIMESTAMP',
                    datetime: 'TIMESTAMP',
                    'timestamp': 'TIMESTAMP',
                    'longchar': 'TEXT',
                    'smallint': 'INTEGER',
                    None: 'TEXT',
                    'nonetype': 'TEXT',
                    }
    else:
        return {}


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


def list_of_dicts_from_query(cursor, sql: str, tablename: str, db_type: str, parameters: list=[], columns: list=[]) -> List[Dict[str, Any]]:
    '''
    Query db using cursor, supplied sql, and tablename.
    Return list of dicts for query result.

    Pass in explicit columns kwarg if you know that returned column order will not match table's column order.
    (This occurs when pulling a subset of columns for example.)
    '''
    try:
        data = cursor.execute(sql, parameters).fetchall()
    except (sqlite3.OperationalError, pyodbc.ProgrammingError, pyodbc.Error) as error:
        print(f'ERROR querying table {tablename}!  Error below:')
        print(error)
        print(f'SQL: {sql}')
        return []

    if not columns:
        if db_type == 'SQLITE':
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
                return [{}]
    return [dict(zip(columns, row)) for row in data]  # table data


def name_clean(name: str) -> bool:
    '''
    Check name and return True if it looks clean (not malicious).
    Return False if it name could be attempting SQL injection.

    Used for table names and column names (as these can't be parameterized).
    '''
    # set for quickly checking possibly malicious characters
    unallowed_characters = {';', '(', ')', '=', '+', "'", '"', '.', '[', ']', ',',
        '{', '}', '\\', '/', '`', '~', '!', '@', '#', '$', '%', '^', '&', '*'}
    for char in name:
        if char in unallowed_characters:
            print(f'ERROR!!!  Prohibited characters detected in:\n  {name}')
            return False
    if 'DROP' in name.upper():
        print(f'ERROR!!!  Prohibited characters detected in:\n  {name}')
        return False
    return True


column_name_changes = set()
def clean_column_name(col_name: str) -> str:
    '''
    Used to ensure column names do not have spaces or forward slashes
    Replace each bad character with an underscore.
    '''
    original_col_name = col_name
    changed = False
    if ' ' in col_name:
        col_name = col_name.replace(' ', '_')
        changed = True
    if '/' in col_name:
        col_name = col_name.replace('/', '_')
        changed = True
    if changed:
        change = f'Column Name {original_col_name} changed to {col_name}'
        if change not in column_name_changes:
            column_name_changes.add(change)
            print(change)
    return col_name


def clean_data(data, columns_and_types, db_type) -> List[dict]:
    '''
    Best effort to clean list of dicts representing database table rows to handle mismatched types,
    null values, and missing columns.
    '''
    columns = list(columns_and_types.keys())
    num_col = len(columns)
    t_map = type_map(db_type)

    for d in data:
        if len(d) != num_col:  # correct missing or extra columns
            missing_columns = [col for col in columns if col not in d]
            for col in missing_columns:
                col_type = columns_and_types[col]
                if similar_type(col_type, 'float'):
                    d[col] = 0
                elif similar_type(col_type, 'str'):
                    d[col] = ''
                else:
                    d[col] = None

            extra_columns = [col for col in d if col not in columns]
            for col in extra_columns:
                del d[col]

        for col, value in d.items():
            d_type = type(value).__name__.lower()
            # now if string d_type, check if it's actually a date that can be handled even though in string format
            if d_type == 'str' and len(value) == 10 and value[4] == '-' and value[7] == '-':  # string like 'YYYY-MM-DD'
                d_type == 'date'

            # now can have case of a timestamp that's a pandas/numpy timestamp which causes problems with sqlite...
            # attempt to convert to python's normal datetime using the .to_pydatetime method in those cases
            if d_type == 'timestamp':
                try:
                    d[col] = value.to_pydatetime()
                except AttributeError:  # nothing happens if not a pandas/numpy timestamp
                    pass

            if not similar_type(columns_and_types[col], t_map[d_type]):  # fix value/type in dict
                if similar_type(columns_and_types[col], 'float'):
                    try:
                        d[col] = float(value if not isinstance(value, str) else value.strip())
                    except (ValueError, TypeError):
                        d[col] = None
                elif similar_type(columns_and_types[col], 'str'):
                    d[col] = str(value)
                else:
                    d[col] = None

            # now if final value is nan, convert to None for more consistent None/null
            if isinstance(d[col], float) and not (d[col] <= 0 or d[col] >= 0):  # is nan
                d[col] = None

    return data


def similar_type(t1, t2) -> bool:
    '''
    Check two type strings and determine if they are close enough to the same type
    for Python/database interaction.
    '''
    t1, t2 = t1.lower(), t2.lower()

    if t1 == t2:
        return True

    numeric = lambda x: True if 'int' in x or 'double' in x or 'float' in x or 'real' in x else False
    if numeric(t1) and numeric(t2):
        return True

    text = lambda x: True if 'text' in x or 'str' in x or 'char' in x else False
    if text(t1) and text(t2):
        return True

    time = lambda x: True if 'time' in x or 'date' in x or 'real' in x or 'text' in x else False
    if time(t1) and time(t2):
        return True

    return False
