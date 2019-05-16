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


    def connection_sqlite(self, also_cursor: bool=False):
        '''
        Return a connection object to the Sqlite3 Database.
        '''
        conn = sqlite3.connect(self.db_location_str)
        if also_cursor:
            return conn, conn.cursor()
        else:
            return conn


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


    def provide_db_connection(func, also_cursor=False):
        '''
        Decorator provides db connection (and cursor if requested) for database.
        To be used to decorate functions that would like to manipulate conn/cursor directly.
        '''
        @wraps(func)  # allows for decorated func's docstring to come through decorator
        def inner(*args, **kwargs):
            if also_cursor:
                conn, cursor = self.connect(also_cursor=True)
                cursor.arraysize = 50  # attempt to speed up cursor.fetchall() calls... not sure of impact
                returned = func(conn, cursor, *args, **kwargs)
            else:
                conn = self.connect()
                returned = func(conn, *args, **kwargs)
            try:
                conn.commit()
            except:
                time.sleep(0.1)  # if database is locked
                conn.commit()
            finally:
                conn.close()
            return returned
        return inner


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



    def __repr__(self) -> str:
        return f'DataBase: {self.db_location_str}'
