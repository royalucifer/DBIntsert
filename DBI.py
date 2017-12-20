"""
@author: Roy Sung
@date:   2017.12.20
@topic:  To insert a Pandas.DataFrame into a SQL table in a fast way.
"""

import psycopg2 as pg
import pandas as pd
import numpy as np
from io import StringIO

types = {
    'DATE':     'DATE',
    'DATETIME': 'TIMESTAMP WITHOUT TIME ZONE',
    'INT':      'BIGINT',
    'FLOAT':    'NUMERIC',
    'VARCHAR':  'TEXT'
}

def df2sql(data, schema, table, con, if_exists='fail'):
    """
    
    The API of class 'DBInsert'.

    Parameters
    -----------------------
    data : Pandas.DataFrame
        the data you want to insert
    schema : str
        the name of schema
    table : str
        the name of table
    con : DBAPI connection
        the connection with your SQL database (Only supported PostgreSQL)
    if_exists : {'fail', 'replace', 'appned'}, default 'fail'
        - fail:    If table exists, do nothing.
        - replace: If table exists, drop it, recreate it, and insert data. Create if does not exist.
        - append:  If table exists, insert data. Create if does not exist.
    
    Examples
    -----------------------
    import psycopg2 as pg
    import DBI

    con  = pg.connect(database="udngpdb", user="gpdbcrmuseronly", password="Welcome1", host="10.206.102.150", port="5432")
    data = pd.read_csv('file_path')

    DBI.df2sql(data=data, schema='dad', table='test', con=con, if_exists='append')

    """
    # check parameters have right values
    if schema == None or table == None:
        raise ValueError("There are no values in 'schema' and 'table'.")
    if con == None:
        raise ValueError("There is no connection to connect with PostgreSQL")
    if data == None or not isinstance(data, pd.DataFrame):
        # data.__class__.__name__ != 'DataFrame'
        raise ValueError("There is no connection to connect with PostgreSQL")

    db = DBInsert(schema=schema, table=table, con=con)
    db.to_sql(df=data, if_exists=if_exists)

class DBInsert():
    """

    Write recores stored in a Pandas.DataFrame to a SQL database in a fast way.

    Parameters
    -----------------------
    schema : str
        the name of schema
    table : str
        the name of table
    con : DBAPI connection
        the connection with your SQL database (Only supported PostgreSQL)

    Examples
    -----------------------
    import psycopg2 as pg

    con = pg.connect(database="udngpdb", user="gpdbcrmuseronly", password="Welcome1", host="10.206.102.150", port="5432")
    db  = DBInsert(schema='dad', table='test', con=con)

    """
    def __init__(self, schema=None, table=None, con=None):
        # if schema == None or table == None:
        #     raise ValueError("There are no values in 'schema' and 'table'.")
        # if con == None:
        #     raise ValueError("There is no connection to connect with PostgreSQL")

        self.con = con
        self.tb_name = '{schema}."{table}"'.format(schema=schema, table=table)

    def execute(self, sql, params={}):
        with self.con.cursor() as cur:
            cur.execute(sql, params)
            self.con.commit()

    def _get_schema(self, df):
        # change th dtypes format from Python to SQL, and create the SQL scripts of "CREATE TABLE..."
        column_types = []
        dtypes = df.dtypes
        for col in dtypes.index:
            dt = dtypes[col]
            if str(dt.type) == "<type 'numpy.datetime64'>":
                sqltype = types['DATETIME']
            elif issubclass(dt.type, np.datetime64):
                sqltype = types['DATETIME']
            elif issubclass(dt.type, (np.integer, np.bool_)):
                sqltype = types['INT']
            elif issubclass(dt.type, np.floating):
                sqltype = types['FLOAT']
            else:
                sampl = df[col][0]
                if str(type(sampl)) == "<type 'datetime.datetime'>":
                    sqltype = types['DATETIME']
                elif str(type(sampl)) == "<type 'datetime.date'>":
                    sqltype = types['DATE']
                else:
                    sqltype = types['VARCHAR']
             
            column_types.append((col, sqltype))

        columns = ',\n  '.join('"{}" {}'.format(x[0], x[1]) for x in column_types)
        template_create = '''
            CREATE TABLE {name} (
                {columns}
            );
        '''    
        create = template_create.format(name=self.tb_name, columns=columns)

        return create

    def _table_exist(self):
        # check the table is still exist
        sql = '''SELECT * FROM {name} LIMIT 1'''.format(name=self.tb_name)
        try:
            df = pd.read_sql_query(sql, self.con)
        except IOError:
            return False
        else:
            exists = True if len(df) > 0 else False
            return exists

    def _pg_copy_from(self, df):
        # append data into existing postgresql table using COPY
    
        # 1. convert df to csv no header
        output = StringIO()
        
        # deal with datetime64 to_csv() bug
        dtypes = df.dtypes
        for col in dtypes.index:
            dt = dtypes[col]
            if str(dt.type) == "<type 'numpy.datetime64'>":
                df[k] = [ v.to_pydatetime() for v in df[k] ]

        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        # contents = output.getvalue()
        
        # 2. copy from
        with self.con.cursor() as cur:
            cur.copy_from(output, self.tb_name)
            self.con.commit()

        return

    def to_sql(self, df, if_exists='fail'):
        """
        
        Parameters
        -----------------------
        df : Pandas.DataFrame
            the data you want to insert
        if_exists : {'fail', 'replace', 'appned'}, default 'fail'
            - fail:    If table exists, do nothing.
            - replace: If table exists, drop it, recreate it, and insert data. Create if does not exist.
            - append:  If table exists, insert data. Create if does not exist.

        Examples
        -----------------------
        import psycopg2 as pg

        con = pg.connect(database="udngpdb", user="gpdbcrmuseronly", password="Welcome1", host="10.206.102.150", port="5432")
        db  = DBI(schema='dad', table='test', con=con)

        data = pd.DataFrame(dict)
        db.to_sql(df=data, if_exists='append')

        """
        # drop table
        if if_exists=='replace' and self._table_exist():
            sql = '''DROP TABLE {name}'''.format(name=self.tb_name)
            self.execute(sql)

        # create table
        if if_exists=='replace' or (if_exists in ('fail', 'append') and not self._table_exist()):
            schema = self._get_schema(df)
            self.execute(schema)

        # insert table
        if if_exists=='fail' and self._table_exist():
            raise ValueError("Table '%s' already exists." % self.tb_name)
        else:
            self._pg_copy_from(df)
