import unittest
import sys
sys.path.insert(1, '..')
import easy_db



class TestSQLite(unittest.TestCase):

    def setUp(self):
        self.db = easy_db.DataBase('test_sqlite3_db.db')

    def test_dbtype(self):
        print(self.db.db_type)
        self.assertTrue(self.db.db_type == 'SQLITE')

    def test_size(self):
        self.assertTrue(self.db.size > 0)

    def test_cols_types(self):
        cols_types = self.db.columns_and_types('TEST_TABLE')
        self.assertTrue(isinstance(cols_types, dict))
        self.assertTrue(len(cols_types) > 0)

    def test_tablename_pull(self):
        tables = self.db.table_names()
        print(tables)
        self.assertTrue(len(tables) == 3)
        self.assertTrue(tables == sorted(tables))

    def test_full_table_pull(self):
        test_table_data = self.db.pull_table('TEST_TABLE')
        print(test_table_data[0])
        self.assertTrue(type(test_table_data) == list)
        self.assertTrue(type(test_table_data[0]) == dict)
        self.assertTrue(len(test_table_data) == 31)

    def test_full_table_pull2(self):
        test_table_data = self.db.pull('TEST_TABLE')
        print(test_table_data[0])
        self.assertTrue(type(test_table_data) == list)
        self.assertTrue(type(test_table_data[0]) == dict)
        self.assertTrue(len(test_table_data) == 31)

    def test_full_table_pull_specific_columns(self):
        test_table_data = self.db.pull_table('TEST_TABLE', columns=('row_id', 'value_1'))
        print(test_table_data[0])
        self.assertTrue(type(test_table_data) == list)
        self.assertTrue(type(test_table_data[0]) == dict)
        self.assertTrue(len(test_table_data) == 31)
        self.assertTrue(len(test_table_data[0].keys()) == 2)

    def test_pull_where_id_in_list(self):
        test_pulled_data = self.db.pull_table_where_id_in_list('THIRD_TABLE', 'parameter', [0.66, 0.67], use_multip=False)
        self.assertTrue(len(test_pulled_data) == 116)
        self.assertTrue(all(d['parameter'] in [0.66, 0.67] for d in test_pulled_data))

    def test_table_creation_and_deletion(self):
        self.db.create_table('TEST_TABLE_CREATION', {'col_1': str, 'col_2': float})
        self.db.append_to_table('TEST_TABLE_CREATION', [{'col_1': 'row_A', 'col_2': 1.5}, {'col_1': 'row_B', 'col_2': 3.7}])
        self.db.drop_table('TEST_TABLE_CREATION')
        self.assertTrue(True)

    def test_table_creation_bad_types(self):
        self.db.create_table('BAD_TYPES', {'col_1': str, 'col_2': str, 'col_3': 'bad_type', 'col_3': tuple()})
        self.assertTrue('BAD_TYPES' not in self.db.table_names())

    def test_progress_callback(self):
        callback = lambda *args: print('Making progress...')
        data = self.db.pull_table('THIRD_TABLE', progress_handler=callback)

    def test_progress_callback_with_n(self):
        callback = lambda *args: print('Making progress 2...')
        data = self.db.pull_table('THIRD_TABLE', progress_handler=(callback, 1000))

    def test_duplicate_deletion(self):
        data = [{'c1': 5, 'c2': 6, 'c3': 7, 'c4': 8}, {'c1': 5, 'c2': 0, 'c3': 7, 'c4': 8}, {'c1': 5, 'c2': 0, 'c3': 3, 'c4': 8}, {'c1': 5, 'c2': 0, 'c3': 3, 'c4': 8}]
        self.db.drop_table('DUP_TABLE')
        self.db.append_to_table('DUP_TABLE', data)
        self.assertTrue(self.db.pull_table('DUP_TABLE') == data)
        self.db.delete_duplicates('DUP_TABLE')
        self.assertTrue(self.db.pull_table('DUP_TABLE', clear_cache=True) == data[:3])
        self.db.delete_duplicates('DUP_TABLE', grouping_columns=['c1', 'c2'])
        self.assertTrue(len(self.db.pull_table('DUP_TABLE', clear_cache=True)) == 2)
        self.db.append_to_table('DUP_TABLE', data)
        self.db.delete_duplicates('DUP_TABLE', grouping_columns=['c1'])
        self.assertTrue(len(self.db.pull_table('DUP_TABLE', clear_cache=True)) == 1)
        self.db.drop_table('DUP_TABLE')

    def test_update(self):
        data = [{'c1': 1, 'c2': 2, 'c3': 3}, {'c1': 11, 'c2': 22, 'c3': 33}]
        self.db.drop_table('UPDATE_TEST')
        self.db.append_to_table('UPDATE_TEST', data)
        self.assertTrue(data == self.db.pull_table('UPDATE_TEST'))
        self.db.update('UPDATE_TEST', 'c1', 1, 'c2', -2)
        self.assertTrue(self.db.pull_table('UPDATE_TEST', clear_cache=True) ==  [{'c1': 1, 'c2': -2, 'c3': 3}, {'c1': 11, 'c2': 22, 'c3': 33}])
        self.db.update('UPDATE_TEST', 'c1', [1, 11], 'c3', [-3, -33])
        self.assertTrue(self.db.pull_table('UPDATE_TEST', clear_cache=True) ==  [{'c1': 1, 'c2': -2, 'c3': -3}, {'c1': 11, 'c2': 22, 'c3': -33}])
        self.db.update('UPDATE_TEST', 'c1', [1, 11], 'c2', 0)
        self.assertTrue(self.db.pull_table('UPDATE_TEST', clear_cache=True) == [{'c1': 1, 'c2': 0, 'c3': -3}, {'c1': 11, 'c2': 0, 'c3': -33}])
        self.db.drop_table('UPDATE_TEST')

    def test_context_manager(self):
        with self.db as cursor:
            cursor.execute('SELECT * FROM TEST_TABLE;')
            data = cursor.fetchall()
        self.assertTrue(len(data)>0)


class TestAccess(unittest.TestCase):

    def setUp(self):
        self.db = easy_db.DataBase('test_db.accdb')

    def test_dbtype(self):
        print(self.db.db_type)
        self.assertTrue(self.db.db_type == 'ACCESS')

    def test_size(self):
        self.assertTrue(self.db.size > 0)

    def test_table_creation_and_deletion(self):
        self.db.create_table('TEST_TABLE_CREATION', {'col_1': str, 'col_2': float})
        self.db.append_to_table('TEST_TABLE_CREATION', [{'col_1': 'row_A', 'col_2': 1.5}, {'col_1': 'row_B', 'col_2': 3.7}])
        self.db.drop_table('TEST_TABLE_CREATION')
        self.assertTrue(True)

    def test_table_creation_bad_types(self):
        self.db.create_table('BAD_TYPES', {'col_1': str, 'col_2': str, 'col_3': 'bad_type', 'col_3': tuple()})
        self.assertTrue('BAD_TYPES' not in self.db.table_names())

    # def test_duplicate_deletion(self):
        # data = [{'c1': 5, 'c2': 6, 'c3': 7, 'c4': 8}, {'c1': 5, 'c2': 0, 'c3': 7, 'c4': 8}, {'c1': 5, 'c2': 0, 'c3': 3, 'c4': 8}, {'c1': 5, 'c2': 0, 'c3': 3, 'c4': 8}]
        # self.db.drop_table('DUP_TABLE')
        # self.db.append_to_table('DUP_TABLE', data)
        # self.assertTrue(self.db.pull_table('DUP_TABLE') == data)
        # self.db.delete_duplicates('DUP_TABLE')
        # self.assertTrue(self.db.pull_table('DUP_TABLE', clear_cache=True) == data[:3])
        # self.db.delete_duplicates('DUP_TABLE', grouping_columns=['c1', 'c2'])
        # self.assertTrue(len(self.db.pull_table('DUP_TABLE', clear_cache=True)) == 2)
        # self.db.append_to_table('DUP_TABLE', data)
        # self.db.delete_duplicates('DUP_TABLE', grouping_columns=['c1'])
        # self.assertTrue(len(self.db.pull_table('DUP_TABLE', clear_cache=True)) == 1)
        # self.db.drop_table('DUP_TABLE')

    def test_append(self):
        with self.db as cursor:
            cursor.execute('DELETE * FROM TEST WHERE ENTITY_NAME=?;', ('ENTITY 2'))

        new = [{'ID': 'LKJLSKDFS', 'STATE': 'NY', 'ENTITY_NAME': 'ENTITY 2', 'SIZE': 34, 'WEIGHT': 431},
                    {'ID': 'EISLSJN', 'STATE': None, 'ENTITY_NAME': 'ENTITY 2', 'SIZE': 93, 'WEIGHT': None},
                    {'ID': 'NVSLJEEIL', 'STATE': 'PA', 'ENTITY_NAME': 'ENTITY 2', 'SIZE': 45, 'WEIGHT': ''},
                    {'ID': 'XOIENKS', 'STATE': 'FL', 'ENTITY_NAME': 'ENTITY 2', 'SIZE': 52, 'WEIGHT': '693.2'},
                    {'ID': 'QLKSDKN', 'STATE': 'WY', 'ENTITY_NAME': 'ENTITY 2', 'WEIGHT': ' 247'},]
        try:
            self.db.append_to_table('TEST', new, robust=False)
            self.assertTrue(False)
        except:
            self.db.append_to_table('TEST', new, robust=True)
            self.assertTrue(True)

    def test_update(self):
        data = [{'c1': 1, 'c2': 2, 'c3': 3}, {'c1': 11, 'c2': 22, 'c3': 33}]
        self.db.drop_table('UPDATE_TEST')
        self.db.append_to_table('UPDATE_TEST', data)
        self.assertTrue(data == self.db.pull_table('UPDATE_TEST'))
        self.db.update('UPDATE_TEST', 'c1', 1, 'c2', -2)
        self.assertTrue(self.db.pull_table('UPDATE_TEST', clear_cache=True) ==  [{'c1': 1, 'c2': -2, 'c3': 3}, {'c1': 11, 'c2': 22, 'c3': 33}])
        self.db.update('UPDATE_TEST', 'c1', [1, 11], 'c3', [-3, -33])
        self.assertTrue(self.db.pull_table('UPDATE_TEST', clear_cache=True) ==  [{'c1': 1, 'c2': -2, 'c3': -3}, {'c1': 11, 'c2': 22, 'c3': -33}])
        self.db.update('UPDATE_TEST', 'c1', [1, 11], 'c2', 0)
        self.assertTrue(self.db.pull_table('UPDATE_TEST', clear_cache=True) == [{'c1': 1, 'c2': 0, 'c3': -3}, {'c1': 11, 'c2': 0, 'c3': -33}])
        self.db.drop_table('UPDATE_TEST')

    def test_context_manager(self):
        with self.db as cursor:
            cursor.execute('SELECT * FROM TEST;')
            data = cursor.fetchall()
        self.assertTrue(len(data)>0)


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
        data = self.db.pull_table('DROP TABLE TEST_TABLE')
        self.assertTrue(data is None)
        data = self.db.pull_table('TEST_TABLE', columns=('row_id;1=1;--', 'value_1'))
        self.assertTrue(data is None)

    def test_similarity(self):
        self.assertTrue(easy_db.util.similar_type('int', 'FLOAT64'))
        self.assertTrue(easy_db.util.similar_type('STR', 'text'))
        self.assertTrue(easy_db.util.similar_type('datetime64', 'DATE   '))




if __name__ == '__main__':
    unittest.main(buffer=True)
