# easy_db

easy_db is a high-level Python library designed to simplify working with databases.  The "DataBase" class handles connecting to various
types of databases while providing simple methods for common tasks.  The underlying database connection and cursor can be used when more
precise control is desired.

# Goals

 - Make common database tasks simple and easy
 - Intelligently handle different database types
 - Provide intuitive, consistent, Pythonic methods database interaction
 - Provide good performance without requiring polished query code
 - Expose database connection and cursor to users wanting fine-grained control
 - Just get the data into Python so we can use it!


# Why use easy_db?

Before easy_db:
```sh
import pyodbc
import os

conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};' +
    r'DBQ=' + os.path.abspath('MyDatabase.accdb') + ';')
cursor = conn.cursor()
cursor.execute('SELECT * FROM test_table;')
data = cursor.fetchall()
columns = [col[0] for col in cursor.description]
table_data = [dict(zip(columns, row)) for row in data]

# table_data -> [{'column1': value1, 'column2': value2}, {...}, ...]
```

Using easy_db:
```sh
import easy_db

db = easy_db.DataBase('MyDatabase.accdb')
table_data = db.pull('test_table')

# table_data -> [{'column1': value1, 'column2': value2}, {...}, ...]
```


# Quick Start

Let's first connect to a SQLite database.
```sh
import easy_db
db = easy_db.DataBase('test_sqlite3_db.db')
```

Now let's see what tables are available in this database.
```sh
tables = db.table_names()
```

Table columns and types are simple to investigate.
```sh
print(db.columns_and_types('example_table'))
```

Let's pull all of the data from a table.  We could start with something like "SELECT * ...", but this is way more fun:
```sh
data = db.pull('example_table')
```

Note that the table/query data is returned as a list of dictionaries with column names as dictionary keys.

 - Pro Tip:  If desired, a Pandas dataframe of the same form as the database table can be easily created from this data structure using:
```sh
import pandas
df = pandas.DataFrame(data)
```

Now perhaps we have an Access database and would like to pull in a table from our SQLite database.  easy_db makes this simple and gracefully handles the nuances of dealing with the different databases.
```sh
db = easy_db.DataBase('test_sqlite3_db.db')
db_2 = easy_db.DataBase('test_access_db.accdb')

db_2.copy_table(db, 'example_table')
```

The DataBase object can be used as a context manager for running custom SQL.  The cursor is provided and the connection runs .commit() and .close() implicitly after the "while" block.
```
with db as cursor:
    cursor.execute('DELETE * FROM example_table;')
```

Thanks for checking out easy_db, and please take a look at the methods of the DataBase class if you'd like to see what other capabilities are available!


License
----
MIT
