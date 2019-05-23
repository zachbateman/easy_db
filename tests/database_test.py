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
        test_table_data = self.database.pull_full_table('TEST_TABLE')
        print(test_table_data[0])
        self.assertTrue(type(test_table_data) == list)
        self.assertTrue(type(test_table_data[0]) == dict)
        self.assertTrue(len(test_table_data) == 31)

    def test_pull_where_id_in_list(self):
        test_pulled_data = self.database.pull_table_where_id_in_list('THIRD_TABLE', 'parameter', [0.66, 0.67], use_multip=False)
        self.assertTrue(len(test_pulled_data) == 116)
        self.assertTrue(all(d['parameter'] in [0.66, 0.67] for d in test_pulled_data))



if __name__ == '__main__':
    unittest.main(buffer=True)
