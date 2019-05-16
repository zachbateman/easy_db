'''
Utility functions for easy_db.
'''
import os



def check_if_file_is_sqlite(filename: str) -> bool:
    '''
    Check if file is a sqlite database.
    See:  https://stackoverflow.com/questions/12932607/how-to-check-if-a-sqlite3-database-exists-in-python
    '''
    if not os.path.isfile(self.db_location_str):
        return False

    if os.path.getsize(filename) < 100:  # SQLite db file header is 100 bytes (minimum file size)
        return False

    with open(self.db_location_str, 'rb') as possible_db_file:
        header = possible_db_file.read(100)

    if header[:16] == 'SQLite format 3\x00':
        return True
    else:
        return False


def list_of_dicts_from_query(cursor, sql: str, tablename: str) -> list:
    '''
    Query db using cursor, supplied sql, and tablename.
    Return list of dicts for query result.
    '''
    data = cursor.execute(sql).fetchall()
    columns = [row.column_name for row in cursor.columns(table=tablename)]
    table_data = [dict(zip(columns, row)) for row in data]
    return table_data
