import unittest
import sys
sys.path.insert(1, '..')
import easy_db



class TestSQLite(unittest.TestCase):

    def setUp(self):
        self.database = easy_db.DataBase('test_sqlite3_db.db')

    def test_dbtype(self):
        print(self.database.db_type)
        self.assertTrue(self.database.db_type == 'SQLITE3')

    def test_tablename_pull(self):
        tables = self.database.pull_all_table_names()
        print(tables)
        self.assertTrue(len(tables) == 3)
        self.assertTrue(tables == sorted(tables))

    def test_full_table_pull(self):
        test_table_data = self.database.pull_table('TEST_TABLE')
        print(test_table_data[0])
        self.assertTrue(type(test_table_data) == list)
        self.assertTrue(type(test_table_data[0]) == dict)
        self.assertTrue(len(test_table_data) == 31)

    def test_full_table_pull_specific_columns(self):
        test_table_data = self.database.pull_table('TEST_TABLE', columns=('row_id', 'value_1'))
        print(test_table_data[0])
        self.assertTrue(type(test_table_data) == list)
        self.assertTrue(type(test_table_data[0]) == dict)
        self.assertTrue(len(test_table_data) == 31)
        self.assertTrue(len(test_table_data[0].keys()) == 2)

    def test_pull_where_id_in_list(self):
        test_pulled_data = self.database.pull_table_where_id_in_list('THIRD_TABLE', 'parameter', [0.66, 0.67], use_multip=False)
        self.assertTrue(len(test_pulled_data) == 116)
        self.assertTrue(all(d['parameter'] in [0.66, 0.67] for d in test_pulled_data))

    def test_table_creation_and_deletion(self):
        self.database.create_table('TEST_TABLE_CREATION', {'col_1': str, 'col_2': float})
        self.database.append_to_table('TEST_TABLE_CREATION', [{'col_1': 'row_A', 'col_2': 1.5}, {'col_1': 'row_B', 'col_2': 3.7}])
        self.database.drop_table('TEST_TABLE_CREATION')
        self.assertTrue(True)

    def test_table_creation_bad_types(self):
        self.database.create_table('BAD_TYPES', {'col_1': str, 'col_2': str, 'col_3': 'bad_type', 'col_3': tuple()})
        self.assertTrue('BAD_TYPES' not in self.database.pull_all_table_names())

    def test_progress_callback(self):
        callback = lambda *args: print('Making progress...')
        data = self.database.pull_table('THIRD_TABLE', progress_handler=callback)

    def test_progress_callback_with_n(self):
        callback = lambda *args: print('Making progress 2...')
        data = self.database.pull_table('THIRD_TABLE', progress_handler=(callback, 1000))

    def test_duplicate_deletion(self):
        data = [{'c1': 5, 'c2': 6, 'c3': 7, 'c4': 8}, {'c1': 5, 'c2': 0, 'c3': 7, 'c4': 8}, {'c1': 5, 'c2': 0, 'c3': 3, 'c4': 8}, {'c1': 5, 'c2': 0, 'c3': 3, 'c4': 8}]
        self.database.drop_table('DUP_TABLE')
        self.database.append_to_table('DUP_TABLE', data)
        self.assertTrue(self.database.pull_table('DUP_TABLE') == data)
        self.database.delete_duplicates('DUP_TABLE')
        self.assertTrue(self.database.pull_table('DUP_TABLE', clear_cache=True) == data[:3])
        self.database.delete_duplicates('DUP_TABLE', grouping_columns=['c1', 'c2'])
        self.assertTrue(len(self.database.pull_table('DUP_TABLE', clear_cache=True)) == 2)
        self.database.append_to_table('DUP_TABLE', data)
        self.database.delete_duplicates('DUP_TABLE', grouping_columns=['c1'])
        self.assertTrue(len(self.database.pull_table('DUP_TABLE', clear_cache=True)) == 1)
        self.database.drop_table('DUP_TABLE')


class TestUtil(unittest.TestCase):

    def setUp(self):
        self.database = easy_db.DataBase('test_sqlite3_db.db')

    def test_name_clean(self):
        self.assertTrue(easy_db.util.name_clean('table'))
        self.assertFalse(easy_db.util.name_clean('Malic10s;--'))
        self.assertFalse(easy_db.util.name_clean('DROP TABLE;'))
        self.assertTrue(easy_db.util.name_clean('TABLE_1'))
        self.assertFalse(easy_db.util.name_clean('{email=dude@test.com}'))
        self.assertFalse(easy_db.util.name_clean('drop'))

    def test_malicious_query(self):
        data = self.database.pull_table('DROP TABLE TEST_TABLE')
        self.assertTrue(data is None)
        data = self.database.pull_table('TEST_TABLE', columns=('row_id;1=1;--', 'value_1'))
        self.assertTrue(data is None)




if __name__ == '__main__':
    unittest.main(buffer=True)
