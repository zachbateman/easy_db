'''
Module containing easy_db DataBase class.
'''
import sqlite3
import pyodbc
import os
import time
from functools import lru_cache
from functools import wraps
from . import util




class DataBase():


    def __init__(self, db_location_str: str='') -> None:
        self.db_location_str = db_location_str

        self.db_type = self.find_db_type()
        if self.db_type == 'ACCESS':
            self.connection = self.connection_access
        elif self.db_type == 'SQL SERVER':
            self.connection = self.connection_sql_server
        elif self.db_type == 'SQLITE3':
            self.connection = self.connection_sqlite



    def find_db_type(self):
        '''
        Figure out what kind of databse is being used.
        '''

        if '.accdb' in self.db_location_str or '.mdb' in self.db_location_str:
            return 'ACCESS'
        elif 'DSN' in self.db_location_str:
            return 'SQL SERVER'
        elif util.check_if_file_is_sqlite(self.db_location_str):
            return 'SQLITE3'
        else:
            return 'Database not recognized!'


    def connection_sqlite(self, also_cursor: bool=False, create_if_none: bool=False):
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


    def connection_access(self, also_cursor: bool=False):
        '''
        Return a connection object to the Access Database.
        '''
        conn = pyodbc.connect(
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};" +
        r'Dbq=' + self.db_location_str + ';')
        if also_cursor:
            return conn, conn.cursor()
        else:
            return conn


    def connection_sql_server(self, also_cursor: bool=False):
        '''
        Return a connection object to the SQL Server Database.
        '''
        conn = pyodbc.connect(self.db_location_str)
        if also_cursor:
            return conn, conn.cursor()
        else:
            return conn


    def provide_db_connection(self, also_cursor=False):
        '''
        Decorator provides db connection (and cursor if requested) for database.
        To be used to decorate functions that would like to manipulate conn/cursor directly.

        Connection commit and closing is handled at the end of this decorator method!
        '''
        def decorator(func):  # need extra layer of scope to handle the also_cursor kwarg passed in user code...
            @wraps(func)  # allows for decorated function's doctstring to come through decorator
            def inner(*args, **kwargs):
                conn = None
                counter = 0
                while conn is None:
                    try:
                        if also_cursor:
                            conn, cursor = self.connection(also_cursor=True)
                            cursor.arraysize = 50  # attempt to speed up cursor.fetchall() calls... not sure of impact
                        else:
                            conn = self.connection()
                    except (pyodbc.Error, sqlite3.OperationalError) as error:  # in case database is locked from another connection
                        time.sleep(0.01)
                        counter += 1
                        if counter > 1000:
                            print(f'ERROR!  Could not access {self.db_location_str}')
                            print('Database is locked from another connection!')
                            break

                returned = func(conn, cursor, *args, **kwargs) if also_cursor else func(conn, *args, **kwargs)

                try:
                    conn.commit()
                except:  # if database is locked
                    time.sleep(0.1)
                    conn.commit()
                finally:
                    conn.close()
                return returned
            return inner
        return decorator


    @lru_cache(maxsize=4)
    def pull_full_table(self, tablename: str) -> list:
        '''
        SELECT * Query for full table as specified from tablename.
        Return list of dicts for rows with column names as keys.
        '''
        sql = f'SELECT * FROM {tablename};'
        conn, cursor = self.connection(also_cursor=True)
        data = util.list_of_dicts_from_query(cursor, sql, tablename, self.db_type)
        conn.close()
        return data


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


    def pull_all_table_names(self) -> list:
        '''
        Return sorted list of all tables in the database.
        '''
        conn, cursor = self.connection(also_cursor=True)
        if self.db_type == 'SQLITE3':
            tables = [tup[0] for tup in cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
        else:
            tables = cursor.tables()

        return sorted(tables)


    def create_table(self, tablename: str, columns_and_types: dict, force_overwrite: bool=False):
        '''
        Create a table in the database with name "tablename"

        Pass in a dictionary containing column names as keys and column types as values.
        values can be tye actual type (like int, float, etc.) or strings of those same (like 'int', 'float', etc.)

        force_overwrite kwarg allows to overwrite existing table if present
        (by default will NOT overwrite/change existing table.)
        '''
        if tablename in self.pull_all_table_names() and not force_overwrite:
            print(f'ERROR!  Cannot create table {tablename} as it already exists!')
            print('Please choose a different name or use force_overwrite=True to overwrite.')

        conn, cursor = self.connection(also_cursor=True)

        if self.db_type == 'ACCESS':
            type_map = {float: 'double',
                                 'float': 'double',
                                 'double': 'double',
                                 int: 'integer',
                                 'int': 'integer',
                                 'integer': 'integer',
                                 str: 'CHAR',
                                 'str': 'CHAR',
                                 'text': 'CHAR',
                                }
            column_types = ', '.join([f'k {type_map[v]}' for k, v in columns_and_types])
            sql = f"CREATE TABLE {tablename}({column_types});"
        elif self.db_type == 'SQLITE3':
            type_map = {float: 'REAL',
                                 'float': 'REAL',
                                 'double': 'REAL',
                                 'real': 'REAL',
                                 int: 'INTEGER',
                                 'int': 'INTEGER',
                                 'integer': 'INTEGER',
                                 str: 'TEXT',
                                 'str': 'TEXT',
                                 'text': 'TEXT',
                                }
            column_types = ', '.join([f'k {type_map[v]}' for k, v in columns_and_types])
            sql = f"CREATE TABLE {tablename}({column_types});"
        else:
            print('ERROR!  Table creation only implemented in SQLite and Access currently.')
            return


        cursor.execute(sql)
        conn.commit()
        conn.close()
        print(f'Table {tablename} successfully created!')



    def __repr__(self) -> str:
        return f'DataBase: {self.db_location_str}'
