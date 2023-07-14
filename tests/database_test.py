import unittest
import sys
sys.path.insert(1, '..')
import easy_db
from easy_db.db_types import DBType
from datetime import datetime


class TestAccess(unittest.TestCase):

    def setUp(self):
        self.db = easy_db.DataBase('test_db.accdb')

    def test_dbtype(self):
        print(self.db.db_type)
        self.assertTrue(self.db.db_type == DBType.ACCESS)

    def test_size(self):
        self.assertTrue(self.db.size > 0)

    def test_table_creation_and_clearing_and_deletion(self):
        self.db.create_table('TEST_TABLE_CREATION', {'col_1': str, 'col_2': float})
        self.db.append('TEST_TABLE_CREATION', [{'col_1': 'row_A', 'col_2': 1.5}, {'col_1': 'row_B', 'col_2': 3.7}])

        table_data = self.db.pull('TEST_TABLE_CREATION')
        self.db.clear_table('TEST_TABLE_CREATION')
        empty_table_data = self.db.pull('TEST_TABLE_CREATION')
        self.assertTrue(empty_table_data == [])
        self.db.append('TEST_TABLE_CREATION', table_data)
        self.assertTrue(table_data == self.db.pull('TEST_TABLE_CREATION'))

        self.db.drop_table('TEST_TABLE_CREATION')
        self.assertTrue(True)

    def test_table_creation_bad_types(self):
        self.db.create_table('BAD_TYPES', {'col_1': str, 'col_2': str, 'col_3': 'bad_type', 'col_3': tuple()})
        self.assertTrue('BAD_TYPES' not in self.db.table_names())

    def test_duplicate_deletion(self):
        data = [{'c1': 5, 'c2': 6, 'c3': 7, 'c4': 8}, {'c1': 5, 'c2': 0, 'c3': 7, 'c4': 8}, {'c1': 5, 'c2': 0, 'c3': 3, 'c4': 8}, {'c1': 5, 'c2': 0, 'c3': 3, 'c4': 8}]
        self.db.drop_table('DUP_TABLE')
        self.db.append('DUP_TABLE', data)
        self.assertTrue(self.db.pull('DUP_TABLE') == data)
        self.db.delete_duplicates('DUP_TABLE')
        self.assertTrue(self.db.pull('DUP_TABLE', fresh=True) == data[:3])
        self.db.delete_duplicates('DUP_TABLE', grouping_columns=['c1', 'c2'])
        self.assertTrue(len(self.db.pull('DUP_TABLE', fresh=True)) == 2)
        self.db.append('DUP_TABLE', data)
        self.db.delete_duplicates('DUP_TABLE', grouping_columns=['c1'])
        self.assertTrue(len(self.db.pull('DUP_TABLE', fresh=True)) == 1)
        self.db.drop_table('DUP_TABLE')

    def test_dup_deletion_dates(self):
        dt = datetime(2021, 1, 1)
        data = [{'c1': 5, 'c2': 6, 'c3': 7, 'c4': dt}, {'c1': 5, 'c2': 0, 'c3': 7, 'c4': dt}, {'c1': 5, 'c2': 0, 'c3': 3, 'c4': dt}, {'c1': 5, 'c2': 0, 'c3': 3, 'c4': dt}]
        self.db.drop_table('DUP_TABLE')
        self.db.append('DUP_TABLE', data)
        self.assertTrue(self.db.pull('DUP_TABLE') == data)
        self.db.delete_duplicates('DUP_TABLE')
        self.assertTrue(self.db.pull('DUP_TABLE', fresh=True) == data[:3])
        self.db.delete_duplicates('DUP_TABLE', grouping_columns=['c1', 'c2'])
        self.assertTrue(len(self.db.pull('DUP_TABLE', fresh=True)) == 2)
        self.db.append('DUP_TABLE', data)
        self.db.delete_duplicates('DUP_TABLE', grouping_columns=['c1'])
        self.assertTrue(len(self.db.pull('DUP_TABLE', fresh=True)) == 1)
        self.db.drop_table('DUP_TABLE')

    def test_append(self):
        with self.db as cursor:
            cursor.execute('DELETE * FROM TEST WHERE ENTITY_NAME=?;', ('ENTITY 2'))

        new = [{'ID': 'LKJLSKDFS', 'STATE': 'NY', 'ENTITY_NAME': 'ENTITY 2', 'SIZE': 34, 'WEIGHT': 431},
                    {'ID': 'EISLSJN', 'STATE': None, 'ENTITY_NAME': 'ENTITY 2', 'SIZE': 93, 'WEIGHT': None},
                    {'ID': 'NVSLJEEIL', 'STATE': 'PA', 'ENTITY_NAME': 'ENTITY 2', 'SIZE': 45, 'WEIGHT': ''},
                    {'ID': 'XOIENKS', 'STATE': 'FL', 'ENTITY_NAME': 'ENTITY 2', 'SIZE': 52, 'WEIGHT': '693.2'},
                    {'ID': 'QLKSDKN', 'STATE': 'WY', 'ENTITY_NAME': 'ENTITY 2', 'WEIGHT': ' 247'},]
        try:
            self.db.append('TEST', new, robust=False)
            self.assertTrue(False)
        except:
            self.db.append('TEST', new, robust=True)
            self.assertTrue(True)

        self.db.append('TEST', {'ID': 'TKALSKJDF', 'STATE': None, 'ENTITY_NAME': 'ENTITY 2', 'SIZE': float('nan'),  'WEIGHT': None}, safe=True, robust=True)

    def test_pull(self):
        data = [{'c1': 1, 'c2': 2, 'c3': 3}, {'c1': 11, 'c2': 22, 'c3': 33}]
        self.db.drop_table('UPDATE_TEST')
        self.db.append('UPDATE_TEST', data)
        self.assertTrue(data == self.db.pull('UPDATE_TEST', cache_conn=True))
        self.db.drop_table('UPDATE_TEST')

    def test_update(self):
        data = [{'c1': 1, 'c2': 2, 'c3': 3}, {'c1': 11, 'c2': 22, 'c3': 33}]
        self.db.drop_table('UPDATE_TEST')
        self.db.append('UPDATE_TEST', data)
        self.assertTrue(data == self.db.pull('UPDATE_TEST'))
        self.db.update('UPDATE_TEST', 'c1', 1, 'c2', -2)
        self.db.update('UPDATE_TEST', 'c1', 1, 'c2', -2, cache_conn=True)
        self.db.update('UPDATE_TEST', 'c1', 1, 'c2', -2, cache_conn=True)
        self.db.update('UPDATE_TEST', 'c1', 1, 'c2', -2, cache_conn=True)
        self.db.update('UPDATE_TEST', 'c1', 1, 'c2', -2)
        self.assertTrue(self.db.pull('UPDATE_TEST', fresh=True) ==  [{'c1': 1, 'c2': -2, 'c3': 3}, {'c1': 11, 'c2': 22, 'c3': 33}])
        self.db.update('UPDATE_TEST', 'c1', [1, 11], 'c3', [-3, -33])
        self.assertTrue(self.db.pull('UPDATE_TEST', fresh=True) ==  [{'c1': 1, 'c2': -2, 'c3': -3}, {'c1': 11, 'c2': 22, 'c3': -33}])
        self.db.update('UPDATE_TEST', 'c1', [1, 11], 'c2', 0)
        self.assertTrue(self.db.pull('UPDATE_TEST', fresh=True) == [{'c1': 1, 'c2': 0, 'c3': -3}, {'c1': 11, 'c2': 0, 'c3': -33}])
        self.db.drop_table('UPDATE_TEST')

    def test_duplicate_append(self):
        existing_id = 'TLAJLKDF'
        data =  [{'ID': existing_id},
                     {'ID': existing_id, 'STATE': 'TEST', 'ENTITY_NAME': 'ENTITY 2'},
                     {'ID': existing_id + '_x', 'STATE': 'TEST', 'ENTITY_NAME': 'ENTITY 2'}]
        self.db.append('TEST', data)
        self.db.append('TEST', data)

    def test_key_columns(self):
        self.assertTrue(self.db.key_columns('TEST') == ['ID'])

    def test_context_manager(self):
        with self.db as cursor:
            cursor.execute('SELECT * FROM TEST;')
            data = cursor.fetchall()
        self.assertTrue(len(data)>0)


class TestMisc(unittest.TestCase):

    def setUp(self):
        self.db = easy_db.DataBase('misc_db.db')
        self.db.append('TEST', [{'a': 5, 'b': 6}, {'a': 8, 'b': 9}, {'a': 10, 'b': 15}])

    def test_execute(self):
        self.db.execute('SELECT * FROM TEST;')
        self.db.execute('CREATE TABLE TEST2(c)')
        self.db.execute('DROP TABLE TEST2')
        self.assertTrue(len(self.db.execute('SELECT * FROM TEST;')) == 3)
        self.db.execute('DELETE from TEST;')
        self.assertTrue(len(self.db.pull('TEST')) == 0)


class TestUtil(unittest.TestCase):

    def setUp(self):
        self.db = easy_db.DataBase('test_sqlite3_db.db')

    def test_name_clean(self):
        self.assertTrue(easy_db.util.name_clean('table'))
        self.assertFalse(easy_db.util.name_clean('Malic10s;--'))
        self.assertFalse(easy_db.util.name_clean('DROP TABLE;'))
        self.assertTrue(easy_db.util.name_clean('TABLE_1'))
        self.assertFalse(easy_db.util.name_clean('{email=dude@test.com}'))
        self.assertFalse(easy_db.util.name_clean('drop'))

    def test_malicious_query(self):
        data = self.db.pull('DROP TABLE TEST_TABLE')
        self.assertTrue(not data)
        data = self.db.pull('TEST_TABLE', columns=('row_id;1=1;--', 'value_1'))
        self.assertTrue(not data)

    def test_similarity(self):
        self.assertTrue(easy_db.util.similar_type('int', 'FLOAT64'))
        self.assertTrue(easy_db.util.similar_type('STR', 'text'))
        self.assertTrue(easy_db.util.similar_type('datetime64', 'DATE   '))



if __name__ == '__main__':
    unittest.main(buffer=True)
