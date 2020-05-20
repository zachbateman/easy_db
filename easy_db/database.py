'''
Module containing easy_db DataBase class.
'''
import sqlite3
import pyodbc
import os
import time
import random
from functools import lru_cache
from functools import wraps
import tqdm
import easy_multip
from . import util




class DataBase():

    def __init__(self, db_location_str: str='', create_if_none: bool=True) -> None:
        self.db_location_str = db_location_str

        self.db_type = self._find_db_type()
        if self.db_type == 'ACCESS':
            self.connection = self._connection_access
        elif self.db_type == 'SQL SERVER':
            self.connection = self._connection_sql_server
        elif self.db_type == 'SQLITE3':
            self.connection = self._connection_sqlite
        elif db_location_str[-3:].lower() == '.db' and create_if_none:
            self.connection = self._connection_sqlite
            self.connection(create_if_none=True)
            self.db_type = 'SQLITE3'
        else:
            print(f'Error: database {db_location_str} not found.')



    def _find_db_type(self):
        '''
        Figure out what kind of databse is being used.
        '''
        if '.accdb' in self.db_location_str.lower() or '.mdb' in self.db_location_str.lower():
            return 'ACCESS'
        elif 'dsn' in self.db_location_str.lower():
            return 'SQL SERVER'
        elif util.check_if_file_is_sqlite(self.db_location_str):
            return 'SQLITE3'
        else:
            return 'Database not recognized!'


    def _connection_sqlite(self, also_cursor: bool=False, create_if_none: bool=False):
        '''
        Return a connection object to the Sqlite3 Database.
        '''
        db_file_exists = True if os.path.isfile(self.db_location_str) else False
        if db_file_exists or create_if_none:
            conn = sqlite3.connect(self.db_location_str)
            if also_cursor:
                return conn, conn.cursor()
            else:
                return conn
        else:
            print(f'The file {self.db_location_str} does not exist.')
            print('Please first create this database or specify create_if_none=True.')


    def _connection_access(self, also_cursor: bool=False):
        '''
        Return a connection object to the Access Database.
        '''
        if not os.path.isfile(self.db_location_str):
            if '.accdb' in self.db_location_str and os.path.isfile(self.db_location_str.replace('.accdb', '.mdb')):
                error_str = '\n  ".accdb" file extension specified, but this file was not found.\n  A ".mdb" Access file was found instead.\n  Please change the specified file extension to use the existing database.\n'
            elif '.mdb' in self.db_location_str and os.path.isfile(self.db_location_str.replace('.mdb', '.accdb')):
                error_str = '\n  ".mdb" file extension specified, but this file was not found.\n  An ".accdb" Access file was found instead.\n  Please change the specified file extension to use the existing database.\n'
            else:
                error_str = '\n  Could not locate the specified Access database.\n'
            raise FileNotFoundError(error_str)

        conn = pyodbc.connect(
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};" +
        r'Dbq=' + self.db_location_str + ';')
        if also_cursor:
            return conn, conn.cursor()
        else:
            return conn


    def _connection_sql_server(self, also_cursor: bool=False):
        '''
        Return a connection object to the SQL Server Database.
        '''
        conn = pyodbc.connect(self.db_location_str)
        if also_cursor:
            return conn, conn.cursor()
        else:
            return conn


    def compact_db(self) -> None:
        '''
        Use "VACUUM" command to defragment and shrink sqlite3 database.
        This can have a big impact after deleting many tables.
        Previous sqlite3 bug requiring connection kwarg
        isolation_level=None appears to be fixed.
        '''
        if self.db_type == 'SQLITE3':
            conn = self.connection()
            conn.execute('VACUUM')
            conn.close()
        else:
            print(f'compact_db() only implemented for SQLite3.')
            print(f'Current database is: {self.db_type}')


    def pull_table(self, tablename: str, columns='all', clear_cache=False, progress_handler=None) -> list:
        '''
        SELECT * Query for full table as specified from tablename.

        Alternatively, pass tuple of column names to "columns" kwarg
        to pull the full table for ONLY those columns.

        NOTE!  This function uses caching to avoid extra queries for the same data.
        clear_cache kwarg provides ability to clear cache and pull data
        with a fresh query.  Set clear_cache=True in the event that the database
        table may have been updated since any previous calls.

        progress_handler kwarg can be used to provide status updates to a callback.

        Return list of dicts for rows with column names as keys.
        '''
        if not hasattr(self, '_pull_table_cache'):
            self._pull_table_cache = {}  # provide caching dictionary if does not yet exist

        if clear_cache:
            self._pull_table_cache = {}
            return self.pull_table(tablename, columns)
        else:
            # check for questionable table/column names
            for name in [tablename] + list(columns):
                if not util.name_clean(name):
                    return

            requested_data_key = f'{tablename}_' + '_'.join(sorted(columns))  # key string for caching db pulls in dict
            if requested_data_key not in self._pull_table_cache:
                if columns == 'all':
                    sql = f'SELECT * FROM "{tablename}";'
                else:
                    sql = f'SELECT {", ".join(columns)} FROM "{tablename}";'
                conn, cursor = self.connection(also_cursor=True)

                if progress_handler is not None:
                    if self.db_type == 'SQLITE3':  # progress_handler only currently working for sqlite
                        conn.set_progress_handler(*progress_handler if type(progress_handler) is tuple else (progress_handler, 100)) # Can use to track progress
                    else:
                        print('progress_handler is only available for use with a SQLite database.')

                self._pull_table_cache[requested_data_key] = util.list_of_dicts_from_query(cursor, sql, tablename, self.db_type)
                conn.close()
            return self._pull_table_cache[requested_data_key]


    def pull_table_where(self, tablename: str, condition: str) -> list:
        '''
        SELECT * WHERE Query for table as specified from tablename and condition
        Return list of dicts for rows with column names as keys.
        '''
        sql = f'SELECT * FROM {tablename} WHERE {condition};'
        conn, cursor = self.connection(also_cursor=True)
        data = util.list_of_dicts_from_query(cursor, sql, tablename, self.db_type)
        conn.close()
        return data


    def pull_table_where_id_in_list(self, tablename: str, id_col: str, match_values: list, use_multip: bool=True, progressbar: bool=True) -> list:
        '''
        Pulls all data from table where id_col value is in the provided match_values_to_use.
        Can use multiprocessing if use_multip specified as True.
        '''
        # if use_multip and len(match_values) >= 500:
            # return _pull_table_using_id_list_multip(match_values, *self.connection(also_cursor=True), tablename, id_col, self.db_type)
        # else:
            # if len(match_values) < 500:
                # print('Less than 500 match_values given to pull_table_using_id_list.  Using single process.')
            # return _pull_table_using_id_list(match_values, *self.connection(also_cursor=True), tablename, id_col, self.db_type)
        if use_multip:
            print('use_multip not yet working in pull_table_where_id_in_list().  Using single process.')
        return _pull_table_using_id_list(match_values, *self.connection(also_cursor=True), tablename, id_col, self.db_type, progressbar=progressbar)


    def pull_all_table_names(self, sorted_list=True) -> list:
        '''
        Return list of all tables in the database.
        list is sorted by default
        '''
        conn, cursor = self.connection(also_cursor=True)
        if self.db_type == 'SQLITE3':
            tables = [tup[0] for tup in cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
        elif self.db_type == 'ACCESS':
            tables = [tup[2] for tup in cursor.tables() if tup[3] == 'TABLE']
        else:
            tables = cursor.tables()

        if sorted_list:
            return sorted(tables)
        else:
            return tables


    def table_columns_and_types(self, tablename: str) -> dict:
        '''
        Return dict of all column: type pairs in specified table.
        '''
        if self.db_type == 'ACCESS':
            conn, cursor = self.connection(also_cursor=True)
            try:
                return {col[3]: col[5].lower() for col in cursor.columns(table=tablename)}
            except UnicodeDecodeError:
                print('\nERROR - Unable to read columns.')
                print('This may occur if using Access database with column descriptions populated.')
                print('Try deleting the column descriptions.\n')
        elif self.db_type == 'SQLITE3':
            conn, cursor = self.connection(also_cursor=True)
            return {col[1]: col[2].lower() for col in cursor.execute(f"PRAGMA TABLE_INFO('{tablename}');").fetchall()}
        else:
            sql = f'SELECT * FROM {tablename} LIMIT 2;'
            conn, cursor = self.connection(also_cursor=True)
            data = util.list_of_dicts_from_query(cursor, sql, tablename, self.db_type)
            conn.close()
            if len(data) == 0:
                print(f'No rows in {tablename}.  Please determine columns and types with another method.')
                return None
            else:
                return {key: type(value).__name__ for key, value in data[0].items()}


    def create_table(self, tablename: str, columns_and_types: dict, force_overwrite: bool=False):
        '''
        Create a table in the database with name "tablename"

        Pass in a dictionary containing column names as keys and column types as values.
        values can be tye actual type (like int, float, etc.) or strings of those same (like 'int', 'float', etc.)

        force_overwrite kwarg allows to overwrite existing table if present
        (by default will NOT overwrite/change existing table.)
        '''
        if self.db_type == 'ACCESS':
            type_map = {float: 'double',
                        'float': 'double',
                        'double': 'double',
                        'float64': 'double',
                        'numpy.float64': 'double',
                        int: 'integer',
                        'int': 'integer',
                        'integer': 'integer',
                        str: 'CHAR',
                        'str': 'CHAR',
                        'text': 'CHAR',
                        }
            column_types = ', '.join([f'{k} {type_map[v]}' for k, v in columns_and_types])
            sql = f"CREATE TABLE {tablename}({column_types});"
        elif self.db_type == 'SQLITE3':
            type_map = {float: 'REAL',
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
                        'datetime': 'DATE',
                        'longchar': 'TEXT',
                        }
            column_types = ', '.join([f'{k} {type_map[v]}' for k, v in columns_and_types.items()])
            sql = f"CREATE TABLE '{tablename}'({column_types});"
        else:
            print('ERROR!  Table creation only implemented in SQLite and Access currently.')
            return

        if tablename in self.pull_all_table_names() and not force_overwrite:
            print(f'ERROR!  Cannot create table {tablename} as it already exists!')
            print('Please choose a different name or use force_overwrite=True to overwrite.')
            return
        elif tablename in self.pull_all_table_names() and force_overwrite:
            self.drop_table(tablename)

        t0, create_complete = time.time(), False
        conn, cursor = self.connection(also_cursor=True)
        while time.time() - t0 < 10:
            try:
                cursor.execute(sql)
                conn.commit()
                create_complete = True
                break
            except sqlite3.OperationalError:
                pass
        conn.close()
        if create_complete:
            print(f'Table {tablename} successfully created.')
        else:
            print(f'Unable to create table "{tablename}" as the database is locked!')


    def drop_table(self, tablename: str):
        '''
        Drop/delete the specified table from the database.
        '''
        if self.db_type == 'SQLITE3':
            t0, drop_complete = time.time(), False
            conn, cursor = self.connection(also_cursor=True)
            while time.time() - t0 < 10:
                try:
                    cursor.execute(f'DROP TABLE IF EXISTS "{tablename}";')
                    conn.commit()
                    drop_complete = True
                    break
                except sqlite3.OperationalError:
                    pass
            conn.close()
            if drop_complete:
                print(f'Table "{tablename}" deleted.')
            else:
                print(f'Unable to drop table "{tablename}" as the database is locked!')
        else:
            print('ERROR!  Table deletion only implemented in SQLite currently.')


    def append_to_table(self, tablename: str, data: list, create_table_if_needed: bool=True, safe=False):
        '''
        Append rows of data to database table.
        Create the table in the database if it doesn't exist if create_table_if_needed is True

        "data" arg is list of row dicts where each row dict contains all columns as keys.
        '''
        if tablename not in self.pull_all_table_names() and create_table_if_needed:
            columns_and_types = {key: type(value).__name__ for key, value in data[0].items()}
            self.create_table(tablename, columns_and_types)
        elif tablename not in self.pull_all_table_names() and not create_table_if_needed:
            print(f'ERROR!  Table "{tablename}" does not exist in database!')
            print('Use create_table_if_needed=True if you would like to create it.')
            return None

        columns = [col for col in self.table_columns_and_types(tablename)]
        data_cols = [col for col in data[0]]
        if data_cols != columns:
            print('Append data column order adjusted to match db table column order.')
            data = [{col: d[col]  for col in columns} for d in data]

        if self.db_type == 'SQLITE3':
            insert_sql = f"INSERT INTO '{tablename}' ({','.join(columns)}) VALUES "
        else:
            insert_sql = f"INSERT INTO [{tablename}] ({', '.join(columns)}) VALUES "
        insert_many_sql = insert_sql + f"({', '.join(['?' for _ in range(len(columns))])});"

        conn, cursor = self.connection(also_cursor=True)

        pbar = tqdm.tqdm(total=len(data))
        original_data_len = len(data)
        while len(data) > 0:
            try:
                if safe:
                    for row in data[-100:]:
                        cursor.execute(insert_sql + "(" + ','.join(["'" + str(row[col]) + "'" for col in columns]) + ");")
                elif not safe:
                    cursor.executemany(insert_many_sql, [tuple(row_dict[col] for col in columns) for row_dict in data[-100:]])
                pbar.update(100 if len(data) >= 100 else len(data))
                data = data[:-100]
            except sqlite3.OperationalError:  # database is locked
                print('database locked; retrying')
                time.sleep(random.random() / 10)
        pbar.close()
        conn.commit()
        conn.close()
        print(f'Data inserted in "{tablename}" -> {"{:,.0f}".format(original_data_len)} rows')


    def copy_table(self, other_easydb, tablename: str, new_tablename: str='', column_case: str='same', progress_handler=None):
        '''
        Copy specified table from other easy_db.DataBase to this DB.
        If desired, column names can be set to be all upper or lower-case
        via column_case kwarg ('upper' = UPPERCASE and 'lower' lowercase)
        '''
        data = other_easydb.pull_table(tablename, clear_cache=True, progress_handler=progress_handler)  # clearing cache to ensure fresh pull
        if column_case.lower() == 'lower':
            columns_and_types = {key.lower(): val for key, val in other_easydb.table_columns_and_types(tablename).items()}
            table_data = [{col.lower(): val for col, val in d.items()} for d in data]
        elif column_case.lower() == 'upper':
            columns_and_types = {key.upper(): val for key, val in other_easydb.table_columns_and_types(tablename).items()}
            table_data = [{col.upper(): val for col, val in d.items()} for d in data]
        else:
            if column_case.lower() != 'same':
                print('Warning!  .copy_table column_case kwarg must be "same", "upper", or "lower".  Defaulting to "same".')
            columns_and_types = other_easydb.table_columns_and_types(tablename)
            table_data = data

        if new_tablename != '':
            tablename = new_tablename
        self.drop_table(tablename)
        self.create_table(tablename, columns_and_types)
        self.append_to_table(tablename, table_data)
        print(f'Table {tablename} copied!')


    def __repr__(self) -> str:
        return f'DataBase: {self.db_location_str}'



def _pull_table_using_id_list(match_values_to_use: list, conn, cursor, tablename: str, id_col: str, db_type: str, progressbar: bool=True) -> list:
    '''
    Pulls all data from table where id_col value is in the provided match_values_to_use.
    Separate function here so easy_multip can be used if desired.
    '''
    @lru_cache(maxsize=4)
    def sql_str(subset_len: int) -> str:
        return f"SELECT * FROM [{tablename}] WHERE {id_col} in ({'?,'.join(['' for _ in range(subset_len)])}?);"

    data: list = []
    if progressbar:
        pbar = tqdm.tqdm(total=len(match_values_to_use))
    while len(match_values_to_use) > 0:
        subset = match_values_to_use[:100]
        sql = sql_str(len(subset))
        data.extend(util.list_of_dicts_from_query(cursor, sql, tablename, db_type, subset))
        match_values_to_use = match_values_to_use[100:]
        if progressbar:
            pbar.update(100)
    if progressbar:
        pbar.update(len(match_values_to_use) % 100)
    conn.close()
    return data
# _pull_table_using_id_list_multip = easy_multip.decorators.use_multip(_pull_table_using_id_list)  # decorate with multiprocessing
