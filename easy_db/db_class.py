'''
Module containing easy_db DataBase class.
'''
import sqlite3
import pyodbc
from functools import lru_cache


class DataBase():


    def __init__(self, db_location_str: str='') -> None:
        self.db_location_str = db_location_str
        self.db_type = self.find_db_type()



    def find_db_type(self):

        return ''


    def connection_sqlite(self):
        '''
        '''
        # FIRST check to see if db file exists?!
        return sqlite3.connect(self.db_location_str)


    def connection_access(self):
        '''
        '''
        conn = pyodbc.connect(
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};" +
        r'Dbq=' + self.db_location_str + ';')
        return conn


    def connection_sql_server(self):
        '''
        '''
        return pyodbc.connect(self.db_location_str)


    @lru_cache(maxsize=4)
    def pull_full_table(self, tablename: str) -> list:
        '''

        '''
        sql = f'SELECT * FROM {tablename};'
        return []


    def pull_table_where(self, tablename: str, condition) -> list:
        '''

        '''
        sql = f'SELECT * FROM {tablename} WHERE {condition};'
        return []



    def __repr__(self) -> str:
        return f'DataBase: {self.db_location_str}'
