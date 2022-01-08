'''
Module containing easy_db DataBase class.
'''
from typing import Union, List
import sqlite3
import pyodbc
import os
import time
import random
from functools import lru_cache
import tqdm
from datetime import datetime
from . import util
# hidden import below: win32com (pip install pywin32) only needed for .compact_db if using Access db.




class DataBase():

    def __init__(self, db_location_str: str='', create_if_none: bool=True) -> None:
        self.db_location_str = db_location_str
        self._pull_cache: dict = {}

        self.db_type = self._find_db_type()
        if self.db_type == 'ACCESS':
            self.connection = self._connection_access
            try:
                self.connection()
            except pyodbc.Error as error:
                print(error)
                print(f'\nERROR with pyodbc!  Unable to connect to Access Database: {self.db_location_str}')
                print('Try checking to ensure consistent 64 or 32 bitness between your Python install and your Access driver.')
                print('If all else fails, try uninstalling and then reinstalling your Microsoft Access driver(s)...\n\n')
        elif self.db_type == 'SQL SERVER':
            self.connection = self._connection_sql_server
        elif self.db_type == 'SQLITE':
            self.connection = self._connection_sqlite
        elif db_location_str[-3:].lower() == '.db' and create_if_none:
            self.connection = self._connection_sqlite
            self.connection(create_if_none=True)
            self.db_type = 'SQLITE'
        else:
            print(f'Error: database {db_location_str} not found.')



    def _find_db_type(self) -> str:
        '''
        Figure out what kind of database is being used.
        '''
        if self.db_location_str in os.environ:  # Environment Variables are case-insensitive
            print(f'{self.db_location_str} found as Environment Variable.  Substituting database path.')
            self.db_location_str = os.environ[self.db_location_str]
            return self._find_db_type()

        if '.accdb' in self.db_location_str.lower() or '.mdb' in self.db_location_str.lower():
            return 'ACCESS'
        elif 'dsn' in self.db_location_str.lower():
            return 'SQL SERVER'
        elif util.check_if_file_is_sqlite(self.db_location_str):
            return 'SQLITE'
        else:
            return 'Database not recognized!'


    def _connection_sqlite(self, also_cursor: bool=False, create_if_none: bool=False):
        '''
        Return a connection object to the Sqlite Database.
        '''
        db_file_exists = True if os.path.isfile(self.db_location_str) else False
        if db_file_exists or create_if_none:
            conn = sqlite3.connect(self.db_location_str, detect_types=sqlite3.PARSE_DECLTYPES)
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

        absolute_path = os.path.abspath(self.db_location_str)  # NEED AN ABSOLUTE PATH FOR PYODBC!!!

        # try to connect a few times if first pass fails
        # may occur if the Access locking/unlocking process is taking longer than usual
        conn, tries = None, 0
        while conn is None:
            try:
                tries += 1
                conn = pyodbc.connect(
                    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};' +
                    r'Dbq=' + absolute_path + ';')
            except pyodbc.Error:
                time.sleep(0.7)  # time delay so Access can hopefully get unlocked
            if tries > 5:
                break

        # now try again one more time to get the pyodbc error message/traceback
        if conn is None:
            conn = pyodbc.connect(
                r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};' +
                r'Dbq=' + absolute_path + ';')

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


    @property
    def size(self):
        '''Return size of database in GB'''
        if self.db_type in ['SQLITE', 'ACCESS']:
            return round(os.path.getsize(self.db_location_str) / 10 ** 9, 6)
        else:
            print('db.size only works for SQLite and Access databases!')


    def compact_db(self) -> None:
        '''
        Uses "VACUUM" command to defragment and shrink SQLite database.
        Uses "Compact & Repair" utility of Access database.
        This can have a big impact after deleting many tables.

        Previous sqlite3 bug requiring connection kwarg
        isolation_level=None appears to be fixed.
        '''
        if self.db_type == 'SQLITE':
            conn = self.connection()
            conn.execute('VACUUM')
            conn.close()
        elif self.db_type == 'ACCESS':
            try:
                import win32com.client
            except ModuleNotFoundError:
                print('Error importing "win32com.client" which is used to compact & repair Access db.')
                print('Try "pip install pywin32", and see if that fixes the issue.')
            access_app = win32com.client.Dispatch("Access.Application")
            dest_path = os.path.join(os.path.dirname(self.db_location_str), 'DB_compacted.mdb' if '.mdb' in self.db_location_str else 'DB_compacted.accdb')
            successful = access_app.CompactRepair(self.db_location_str, dest_path)
            if successful:
                print('Compact & Repair SUCCESSFUL')
                os.remove(self.db_location_str)
                os.rename(dest_path, self.db_location_str)
            else:
                print('Compact & Repair FAILED')
                os.remove(dest_path)
            access_app = None
        else:
            print('compact_db() only implemented for SQLite and Access databases.')
            print(f'Current database is: {self.db_type}')


    def execute(self, sql: str, parameters: list=[]):
        '''
        Shortcut for cursor.execute().

        Return similar to how cursor.execute() behaves.  That is, if a SELECT query, return selected data.
        If no return data, return None.
        '''
        with self as cursor:
            query = cursor.execute(sql, parameters)
            try:
                result = query.fetchall()
                return result if result else None
            except:
                return None


    def pull(self, tablename: str, columns='all', fresh=False, progress_handler=None) -> list:
        '''
        "SELECT *" query for full table as specified from tablename.
        ALSO WORKS for an Access Select query named tablename!

        Alternatively, pass tuple of column names to "columns" kwarg
        to pull the full table for ONLY those columns.

        NOTE!  This function uses caching to avoid extra queries for the same data.
        "fresh" kwarg provides ability to clear cache and pull data
        with a fresh query.  Set fresh=True in the event that the database
        table may have been updated since any previous calls.

        progress_handler kwarg can be used to provide status updates to a callback.
        progress_handler type can be either a callback function or a 2-tuple
        where the first item is the callback and the second item is the "n" arg passed
        to the sqlite3 conn.set_progress_handler function that specifies
        the interval at which the callback is called. (# of SQLite instructions)
        Basically, a larger "n" value reduces the number of callbacks.

        Return list of dicts for rows with column names as keys.
        '''
        if fresh:
            self._clear_pull_cache(tablename)  # clear cache for this table
            return self.pull(tablename, columns, progress_handler=progress_handler)

        else:
            if columns == 'all':
                requested_data_key = tablename
            else:
                requested_data_key = f'{tablename}_' + '_'.join(sorted(columns))  # key string for caching db pulls in dict

            try:
                return self._pull_cache[requested_data_key]

            except KeyError:
                # check for questionable table/column names
                for name in [tablename] + list(columns):
                    if not util.name_clean(name):
                        return []

                # ensure specified tablename is a valid table (or query possibly in Access)
                if tablename not in self.table_names() + self.query_names():
                    print(f'Table or query "{tablename}" not found.  Pull aborted.')
                    return []

                if columns == 'all':
                    sql = f'SELECT * FROM "{tablename}";'
                elif isinstance(columns, str):
                    columns = [columns]  # convert to list for a single user-provided column string
                else:
                    sql = f'SELECT {", ".join(columns)} FROM "{tablename}";'
                conn, cursor = self.connection(also_cursor=True)

                if progress_handler is not None:
                    if self.db_type == 'SQLITE':  # progress_handler only currently working for sqlite
                        conn.set_progress_handler(*progress_handler if type(progress_handler) is tuple else (progress_handler, 100))  # Can use to track progress
                    else:
                        print('progress_handler is only available for use with a SQLite database.')

                self._pull_cache[requested_data_key] = util.list_of_dicts_from_query(cursor, sql, tablename, self.db_type, columns=columns if isinstance(columns, list) else [])
                conn.close()
                return self._pull_cache[requested_data_key]


    def _clear_pull_cache(self, tablename) -> None:
        '''Fully clear pull cache for all keys related to the specified table.'''
        for key in list(self._pull_cache.keys()):
            if tablename in key:
                self._pull_cache.pop(key, None)


    def pull_where(self, tablename: str, condition: str, columns='all') -> list:
        '''
        SELECT * WHERE Query for table as specified from tablename and condition
        Return list of dicts for rows with column names as keys.
        '''
        if columns == 'all':
            sql = f'SELECT * FROM {tablename} WHERE {condition};'
        elif isinstance(columns, str):
            columns = [columns]  # convert to list for a single user-provided column string
        elif isinstance(columns, list):  # list of columns to pull
            sql = f'SELECT {", ".join(columns)} FROM {tablename} WHERE {condition};'
        else:
            print('Columns kwarg for .pull_where must be a list of column names.')
        conn, cursor = self.connection(also_cursor=True)
        data = util.list_of_dicts_from_query(cursor, sql, tablename, self.db_type, columns=columns if isinstance(columns, list) else [])
        conn.close()
        return data


    def pull_where_id_in_list(self, tablename: str, id_col: str, match_values: list, columns='all', use_multip: bool=False, progressbar: bool=False) -> list:
        '''
        Pulls all data from table where id_col value is in the provided match_values.
        '''
        if isinstance(columns, str) and columns != 'all':
            columns = [columns]

        @lru_cache(maxsize=4)
        def sql_str(subset_len: int) -> str:
            if columns == 'all':
                return f"SELECT * FROM [{tablename}] WHERE {id_col} in ({'?,'.join(['' for _ in range(subset_len)])}?);"
            else:
                return f"SELECT {', '.join(columns)} FROM [{tablename}] WHERE {id_col} in ({'?,'.join(['' for _ in range(subset_len)])}?);"

        if progressbar:
            pbar = tqdm.tqdm(total=len(match_values))

        conn, cursor = self.connection(also_cursor=True)
        data: list = []
        while len(match_values) > 0:
            subset = match_values[:100]
            sql = sql_str(len(subset))
            data.extend(util.list_of_dicts_from_query(cursor, sql, tablename, self.db_type, subset, columns=columns if isinstance(columns, list) else []))
            match_values = match_values[100:]
            if progressbar:
                pbar.update(100)

        if progressbar:
            pbar.update(len(match_values) % 100)

        conn.close()
        return data


    @lru_cache(maxsize=1)
    def table_names(self) -> list:
        '''
        Return sorted list of all tables in the database.
        '''
        conn, cursor = self.connection(also_cursor=True)
        if self.db_type == 'SQLITE':
            tables = [tup[0] for tup in cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
        elif self.db_type == 'ACCESS':
            tables = [tup[2] for tup in cursor.tables() if tup[3] == 'TABLE']
        else:
            tables = cursor.tables()
        return sorted(tables)


    @lru_cache(maxsize=1)
    def query_names(self) -> list:
        '''
        Return sorted list of all queries in the database.
        Only works for Access Select queries.
        '''
        if self.db_type == 'ACCESS':
            conn, cursor = self.connection(also_cursor=True)
            return sorted(tup[2] for tup in cursor.tables() if tup[3] == 'VIEW')
        else:
            return []


    @lru_cache(maxsize=64)
    def columns_and_types(self, tablename: str) -> dict:
        '''
        Return dict of all column: type pairs in specified table.
        '''
        if self.db_type == 'ACCESS':
            conn, cursor = self.connection(also_cursor=True)
            try:
                return {col[3]: col[5].lower() for col in cursor.columns(table=tablename)}
            except UnicodeDecodeError:
                print(f'\nERROR - Unable to read columns for table: {tablename}.')
                print('This may occur if using Access database with column descriptions populated.')
                print('Try deleting the column descriptions.\n')
                return {}
        elif self.db_type == 'SQLITE':
            conn, cursor = self.connection(also_cursor=True)
            return {col[1]: col[2].lower() for col in cursor.execute(f"PRAGMA TABLE_INFO('{tablename}');").fetchall()}
        else:
            sql = f'SELECT * FROM {tablename} LIMIT 2;'
            conn, cursor = self.connection(also_cursor=True)
            data = util.list_of_dicts_from_query(cursor, sql, tablename, self.db_type)
            conn.close()
            if len(data) == 0:
                print(f'No rows in {tablename}.  Please determine columns and types with another method.')
                return {}
            else:
                return {key: type(value).__name__.lower() for key, value in data[0].items()}


    def key_columns(self, tablename: str) -> list:
        '''
        Return the columns of the specified table that are primary keys.'
        '''
        if not self.db_type == 'ACCESS':
            print('ERROR!  .key_columns is only currently implemented for Access databases.')
        with self as cursor:
            key_cols = [row[8] for row in cursor.statistics(tablename) if row[5] and 'key' in row[5].lower()]
        return key_cols


    def create_table(self, tablename: str, columns_and_types: dict, force_overwrite: bool=False) -> None:
        '''
        Create a table in the database with name "tablename"

        Pass in a dictionary containing column names as keys and column types as values.
        values can be tye actual type (like int, float, etc.) or strings of those same (like 'int', 'float', etc.)

        force_overwrite kwarg allows to overwrite existing table if present
        (by default will NOT overwrite/change existing table.)
        '''
        type_map = util.type_map(self.db_type)
        if not type_map:
            print('ERROR!  Table creation only implemented in SQLite and Access currently.')
            return

        columns_and_types = {util.clean_column_name(col): v.lower() if isinstance(v, str) else v for col, v in columns_and_types.items()}  # make sure column names are good
        try:
            column_types = ', '.join([f'[{col}] {type_map[v]}' for col, v in columns_and_types.items()])
        except KeyError:
            type_values = set(columns_and_types.values())
            keys_not_in_type_map = [str(type_val) for type_val in type_values if type_val not in type_map]
            print(f'ERROR in easy_db!  Unexpected type(s): {", ".join(keys_not_in_type_map)} in columns_and_types.')
            print('-> Please submit a pull request adding these types to the .create_table type_maps!\n')
            return

        if self.db_type == 'ACCESS':
            sql = f"CREATE TABLE {tablename}({column_types});"
        elif self.db_type == 'SQLITE':
            sql = f"CREATE TABLE '{tablename}'({column_types});"

        if tablename in self.table_names() and not force_overwrite:
            print(f'ERROR!  Cannot create table {tablename} as it already exists!')
            print('Please choose a different name or use force_overwrite=True to overwrite.')
            return
        elif tablename in self.table_names() and force_overwrite:
            self.drop_table(tablename)

        t0, create_complete = time.time(), False
        conn, cursor = self.connection(also_cursor=True)
        while time.time() - t0 < 10:
            try:
                cursor.execute(sql)
                conn.commit()
                create_complete = True
                break
            except sqlite3.OperationalError as error:  # storing 'error' variable for printing after loop if create_complete == False
                pass
        conn.close()
        if create_complete:
            print(f'Table {tablename} successfully created.')
            self.table_names.cache_clear()  # need to repull table names as just created a new one
            self.columns_and_types.cache_clear()
        else:
            print(error)
            print(f'\nUnable to create table "{tablename}"\nPerhaps the database is locked?!')


    def _check_potential_duplicates(self, tablename: str, data: list) -> set:
        '''
        Check for rows in data arg that would be error-causing duplicates
        (same primary keys) as existing database rows.
        Return set of key-tuples from existing rows to be avoided.
        '''
        keys = self.key_columns(tablename)
        if not keys:
            return set()
        else:
            existing_tups = set(tuple(row[key] for key in keys) for row in self.pull(tablename, columns=keys, fresh=True))
            dups = set()
            for d in data:
                tup = tuple(d[key] for key in keys)
                if tup in existing_tups:
                    dups.add(tup)
            return dups


    def append(self, tablename: str, data: Union[List[dict], dict], create_table_if_needed: bool=True, safe=False, clean_column_names=False, robust: bool=True, progressbar: bool=None) -> None:
        '''
        Append rows of data to database table.
        Create the table in the database if it doesn't exist if create_table_if_needed is True

        "data" arg is list of row dicts where each row dict contains all columns as keys.

        "safe" kwarg is False by default and parameterized insert queries are used.
        IF you know the data is ~~~VERY SAFE~~~ without a chance of SQL Injection...
        ...then setting safe=True converts data into direct SQL strings for faster imports.
        Note that safe=True is more delicate and sensitive to the quality of data.

        "robust" kwarg enables automatic data cleaning (type conversions, null, missing columns) if True.
        Setting robust to False improves speed if using clean input data.
        '''
        if not data:  # check to ensure provided data actually contains rows of data
            print('No data provided to append.')
            return

        if progressbar is None:
            if len(data) < 1000:
                progressbar = False
            else:
                progressbar = True

        if isinstance(data, dict):  # handle case of single row append by converting it to a list
            data = [data]

        if clean_column_names:
            for key in list(data[0].keys()):
                for row in data:
                    row[util.clean_column_name(key)] = row.pop(key)

        if tablename not in self.table_names() and create_table_if_needed:
            self.create_table(tablename, {key: type(value).__name__ for key, value in data[0].items()})
        elif tablename not in self.table_names() and not create_table_if_needed:
            print(f'ERROR!  Table "{tablename}" does not exist in database!\nUse create_table_if_needed=True if you would like to create it.')
            return None

        if robust:
            data = util.clean_data(data, self.columns_and_types(tablename), self.db_type)

        columns = [col for col in self.columns_and_types(tablename)]
        data_cols = [col for col in data[0]]
        if data_cols != columns:
            try:
                data = [{col: d[col] for col in columns} for d in data]
            except KeyError:
                print(f'Error!  Table {tablename} columns do not match the keys of the data to be appended.')
                print('Try setting robust=True and/or /n  set clean_column_names=True to replace " " and "/" with underscores in data keys.')
                return

        if self.db_type == 'SQLITE':
            insert_sql = f"INSERT INTO '{tablename}' ({','.join([f'[{col}]' for col in columns])}) VALUES "
        else:
            insert_sql = f"INSERT INTO [{tablename}] ({', '.join([f'[{col}]' for col in columns])}) VALUES "
        insert_many_sql = insert_sql + f"({', '.join(['?' for _ in range(len(columns))])});"

        # Check for potential duplicate (key) entries if Access to avoid pyodbc error and crash of whole append.
        if self.db_type == 'ACCESS' and robust:
            dup_rows = self._check_potential_duplicates(tablename, data)
            key_cols = self.key_columns(tablename)
            if dup_rows:
                non_dup_data = [d for d in data if tuple(d[key] for key in key_cols) not in dup_rows]
                skip_count = len(data) - len(non_dup_data)
                print(f"\n{skip_count} row{'s were' if skip_count > 1 else ' was'} skipped in .append due to being primary key duplicates\n  of rows that already exist in table: {tablename}")
                if not non_dup_data:
                    print('No remaining data to append.')
                    return
                print(f'The remaining {len(non_dup_data)} rows are still being appended.\n')
                data = non_dup_data

        is_sqlite = True if self.db_type == 'SQLITE' else False
        is_access = True if self.db_type == 'ACCESS' else False
        def convert_to_sql(value):
            if value is None:
                return 'NULL'
            elif isinstance(value, str):
                return f"'{value}'"
            elif isinstance(value, datetime) and is_sqlite:
                return f"'{value}'"
            elif isinstance(value, datetime) and is_access:
                return f'#{value}#'  # adding "#" on either end makes the Access date insert works
            else:
                return value

        conn, cursor = self.connection(also_cursor=True)
        if progressbar:
            pbar = tqdm.tqdm(total=len(data))
        original_data_len = len(data)
        retry_attempts = 0
        while len(data) > 0:
            try:
                if safe:
                    for row in data[-100:]:
                        cursor.execute(insert_sql + '(' + ','.join([f'{convert_to_sql(row[col])}' for col in columns]) + ');')
                else:
                    try:
                        cursor.executemany(insert_many_sql, [tuple(row_dict[col] for col in columns) for row_dict in data[-100:]])
                    except (pyodbc.IntegrityError, sqlite3.InterfaceError):
                        # this section is just intended to help debug issues with input data by printing problematic data
                        # pyodbc.IntegrityError may occur if null value provided for index/primary key column
                        # sqlite3.InterfaceError may occur if an unsupported data type is provided
                        for row_dict in data[-100:]:
                            try:
                                cursor.executemany(insert_many_sql, [tuple(row_dict[col] for col in columns)])
                            except (pyodbc.IntegrityError, sqlite3.InterfaceError):
                                print('\n\n\n' + '-'*50 + 'ERROR!  Triggering input row shown below:')
                                for key, val in row_dict.items():
                                    print(f'    {col.ljust(15)}   |   {val}')
                                print('-'*50 + '\n')
                                cursor.executemany(insert_many_sql, [tuple(row_dict[col] for col in columns)])  # call again to trigger exception messaging and exit

                if progressbar:
                    pbar.update(100 if len(data) >= 100 else len(data))
                data = data[:-100]
            except sqlite3.OperationalError as error:  # database is locked
                if retry_attempts < 5:
                    retry_attempts += 1
                    print('Database locked?  Retrying...')
                    time.sleep(random.random() / 10)
                else:
                    print(error)
                    break
        if progressbar:
            pbar.close()
        conn.commit()
        conn.close()
        self._clear_pull_cache(tablename)  # clear cache for this table as want new table pull if something has been updated
        print(f'Data inserted in "{tablename}" -> {"{:,.0f}".format(original_data_len)} rows')


    def update(self, tablename: str, match_col: str, match_val, update_col: str, update_val, progress_handler=None) -> None:
        '''
        Update a database table with a value or values.

        match_col arg specifies the column used for filtering/matching rows of the table.
        match_val is the value or values used to filter the table.  A single value, or an iterable (list or tuple) of values can be provided.

        update_col is... the column to be updated.
        update_val can be a single value or an iterable (list or tuple) of values.

        If single match_val and update_val args are specified, the table will be updated for a single cell
        or several depending on if match_col[match_val] ends up with one row or more than one.

        If iterable match_val and update_val args are provided, THESE ITERABLES MUST BE THE SAME LENGTH.
        Additionally, the match_col must be UNIQUE/KEY identifiers so that each match_val corresponds to ONLY one row.

        progress_handler kwarg can be used to provide status updates to a callback.
        progress_handler type can be either a callback function or a 2-tuple
        where the first item is the callback and the second item is the "n" arg passed
        to the sqlite3 conn.set_progress_handler function that specifies
        the interval at which the callback is called. (# of SQLite instructions)
        Basically, a larger "n" value reduces the number of callbacks.
        '''
        # Abort update if match or update column does not exist in the table (may be misspelled or just missing)
        table_columns = set(self.columns_and_types(tablename).keys())
        for col in (match_col, update_col):
            if col not in table_columns:
                print(f'UPDATE FAILED!  Column "{col}" not in {tablename}.')
                return

        conn, cursor = self.connection(also_cursor=True)

        if progress_handler is not None:
            if self.db_type == 'SQLITE':  # progress_handler only currently working for sqlite
                conn.set_progress_handler(*progress_handler if type(progress_handler) is tuple else (progress_handler, 100))  # Can use to track progress
            else:
                print('progress_handler is only available for use with a SQLite database.')

        sql = f'UPDATE {tablename} SET [{update_col}]=? WHERE [{match_col}]=?;'  # can't pass column names in execute statement, just values
        if isinstance(match_val, (list, tuple)):
            if isinstance(update_val, (list, tuple)):  # many rows to update
                if len(match_val) != len(update_val) and not isinstance(update_val, str):  # many rows to update with same number of values
                    print('ERROR!  The number of match values must equal the number of update values!')
                    return
                for m_val, u_val in tqdm.tqdm(zip(match_val, update_val), total=len(match_val)):
                    cursor.execute(sql, (u_val, m_val))
            else:  # case of many rows to update with same value
                for m_val in tqdm.tqdm(match_val, total=len(match_val)):
                    cursor.execute(sql, (update_val, m_val))
        else:
            cursor.execute(sql, (update_val, match_val))
        conn.commit()
        self._clear_pull_cache(tablename)  # clear cache for this table as want new table pull if something's been updated


    def add_column(self, tablename: str, new_col: str, new_type='str') -> None:
        '''Add a new column to a database table.'''
        if new_col in self.columns_and_types(tablename):
            print(f'Column {new_col} is already in {tablename}!')
            return
        if new_type == 'str':
            new_type = 'varchar(255)' if self.db_type == 'ACCESS' else 'TEXT'
        with self as cursor:
            cursor.execute(f'ALTER TABLE {tablename} ADD COLUMN {new_col} {new_type};')
        print(f'Column {new_col} added to {tablename}.')
        self._clear_pull_cache(tablename)  # clear cache for this table as want new table pull if something's been updated
        self.columns_and_types.cache_clear()


    def drop_column(self, tablename: str, column: str) -> None:
        '''Remove a column from a database table.'''
        if column not in self.columns_and_types(tablename):
            print(f'Column {column} does not exist in {tablename}.')
            return

        with self as cursor:
            cursor.execute(f'ALTER TABLE {tablename} DROP COLUMN "{column}";')

        print(f'Column {column} removed from {tablename}')
        self._clear_pull_cache(tablename)  # clear cache for this table as want new table pull if something's been updated
        self.columns_and_types.cache_clear()


    def delete_duplicates(self, tablename: str, grouping_columns=None) -> None:
        '''
        Delete duplicate rows from a db table while retaining most recently added row.
        Duplicates are determined by grouping based on the grouping_columns kwarg (provide iterable).
        If grouping_columns is not provided, all columns are used (rows must match perfectly).
        '''
        if self.db_type not in ['SQLITE', 'ACCESS']:
            print('.delete_duplicates currently only implemented for SQLite and Access databases.')
            return

        print(f'Deleting duplicate rows from {tablename}.  Please wait...')

        if grouping_columns is None:
            grouping_columns = sorted(self.columns_and_types(tablename).keys())

        if self.db_type == 'SQLITE':
            with self as cursor:
                cursor.execute(f'DELETE FROM {tablename} WHERE rowid NOT IN (SELECT max(rowid) FROM {tablename} GROUP BY {", ".join(grouping_columns)})')

        elif self.db_type == 'ACCESS':
            # TODO:  Think some sort of SQL can accomplish dup deletion better than in Python... haven't figured it out yet
            # with self as cursor:
                # cursor.execute(f'DELETE * FROM {tablename} WHERE rowid NOT IN (SELECT max(rowid) FROM {tablename} GROUP BY {", ".join(grouping_columns)})')
                # cursor.execute(f'DELETE * FROM {tablename} WHERE rowid NOT IN (SELECT DISTINCT {", ".join(grouping_columns)} FROM {tablename})')
                # cursor.execute(f'DELETE {tablename}.* WHERE NOT EXISTS (SELECT DISTINCT {", ".join(grouping_columns)} FROM {tablename})')
                # sql = f'SELECT DISTINCT {", ".join(grouping_columns)} FROM {tablename}'
                # print(sql)
                # print(cursor.execute(sql).fetchall())
                # sql = f'DELETE * FROM {tablename} WHERE {", ".join(grouping_columns)} NOT IN (SELECT DISTINCT {", ".join(grouping_columns)} FROM {tablename})'
                # sql = f'DELETE * FROM {tablename} WHERE NOT EXISTS (SELECT DISTINCT {", ".join(grouping_columns)} FROM {tablename})'
                # sql = f'DELETE * FROM {tablename} WHERE EXISTS LEFT JOIN {tablename} ON (SELECT DISTINCT {", ".join(grouping_columns)} FROM {tablename})'
                # print(sql)
                # cursor.execute(f'DELETE * FROM {tablename} WHERE {", ".join(grouping_columns)} NOT IN (SELECT DISTINCT {", ".join(grouping_columns)} FROM {tablename})')
                # cursor.execute(sql)

            data = self.pull(tablename)
            existing_combos = set()
            new_data = []
            for row in reversed(data):
                row_combo = tuple(row[col] for col in grouping_columns)
                if row_combo not in existing_combos:
                    new_data.append(row)
                    existing_combos.add(row_combo)
            with self as cursor:
                cursor.execute(f'DELETE * FROM {tablename};')
            self.append(tablename, list(reversed(new_data)), safe=True, robust=False)  # UN-reverse table entries

        self._clear_pull_cache(tablename)  # clear cache for this table as want new table pull if something has been updated


    def create_index(self, tablename: str, column: str, index_name: str='', unique: bool=False) -> None:
        if self.db_type == 'SQLITE':
            index_name = column if index_name == '' else index_name  # use column name if not provided
            with self as cursor:
                cursor.execute(f'CREATE {"UNIQUE " if unique else ""}INDEX {index_name} on {tablename}({column});')
            self.columns_and_types.cache_clear()
        else:
            print('.create_index is currently only implemented for SQLite databases.')


    def drop_table(self, tablename: str) -> None:
        '''
        Drop/delete the specified table from the database.
        '''
        if tablename not in self.table_names():
            print(f'Table "{tablename}" does not exist.  Table drop aborted.')
            return

        if self.db_type == 'SQLITE':
            t0, drop_complete = time.time(), False
            with self as cursor:
                while time.time() - t0 < 10:
                    try:
                        cursor.execute(f'DROP TABLE IF EXISTS "{tablename}";')
                        drop_complete = True
                        break
                    except sqlite3.OperationalError:
                        pass

            if drop_complete:
                print(f'Table "{tablename}" deleted.')
            else:
                print(f'Unable to drop table "{tablename}" as the database is locked!')
        elif self.db_type == 'ACCESS':
            with self as cursor:
                cursor.execute(f'DROP TABLE {tablename};')
            print(f'Table {tablename} deleted.')
        else:
            print('ERROR!  Table deletion only implemented in SQLite and Access currently.')
            return

        self._clear_pull_cache(tablename)  # clear cache for this table as table has been dropped
        self.table_names.cache_clear()  # need to repull table names as just (likely) deleted one
        self.columns_and_types.cache_clear()


    def copy_table(self, other_db, tablename: str, new_tablename: str='', column_case: str='same', progress_handler=None) -> None:
        '''
        Copy specified table from other easy_db.DataBase to this DB.
        If desired, column names can be set to be all upper or lower-case
        via column_case kwarg ('upper' = UPPERCASE and 'lower' lowercase)
        '''
        if tablename not in other_db.table_names():
            print(f'Table "{tablename}" not found.  Table copy aborted.')
            return

        data = other_db.pull(tablename, fresh=True, progress_handler=progress_handler)  # clearing cache to ensure fresh pull
        if column_case.lower() == 'lower':
            columns_and_types = {key.lower(): val for key, val in other_db.columns_and_types(tablename).items()}
            table_data = [{col.lower(): val for col, val in d.items()} for d in data]
        elif column_case.lower() == 'upper':
            columns_and_types = {key.upper(): val for key, val in other_db.columns_and_types(tablename).items()}
            table_data = [{col.upper(): val for col, val in d.items()} for d in data]
        else:
            if column_case.lower() != 'same':
                print('Warning!  .copy_table column_case kwarg must be "same", "upper", or "lower".  Defaulting to "same".')
            columns_and_types = other_db.columns_and_types(tablename)
            table_data = data

        if new_tablename != '':
            tablename = new_tablename
        self.drop_table(tablename)
        self.create_table(tablename, columns_and_types)
        if table_data:
            self.append(tablename, table_data)
        print(f'Table {tablename} copied!')
        self.columns_and_types.cache_clear()


    def __repr__(self) -> str:
        return f'DataBase: {self.db_location_str}'


    def __enter__(self):
        self.context_conn, cursor = self.connection(also_cursor=True)
        return cursor


    def __exit__(self, *args):
        self.context_conn.commit()
        self.context_conn.close()
