import time

import pandas as pd
import sqlalchemy
from pyhive import presto
from sqlalchemy import create_engine


class PrestoUnit:
    def __init__(self, username: str, password: str, schema: str = 'default', host: str = 'emra1.harbdata.com',
                 port: int = '10000') -> None:
        self.username = username
        self.password = password
        self.schema = schema
        self.host = host
        self.port = port

    def create_conn_sqlalchemy(self):
        engine_info = 'presto://%s:%s/hive/%s' % (
            self.host, self.port, self.schema)
        print(engine_info)

        # return the engine
        conn = sqlalchemy.create_engine(engine_info)
        return conn

    def create_conn_pyhive(self):
        conn = presto.Connection(host=self.host, port=self.port, username=self.username, schema=self.schema)
        return conn

    def _execute_one(self, query):
        conn = self.create_conn_pyhive()
        cursor = conn.cursor()
        cursor.execute(query)
        response = cursor.fetchall()
        while not response:
            time.sleep(5)
            response = cursor.fetchall()

    def execute(self, query):
        queries = query.split(';')
        for q in queries:
            if q.strip():
                self._execute_one(q)

    def read_sql(self, query):
        query = query.replace('%Y%m%d', '%%Y%%m%%d')
        query = query.replace(';', '')
        conn = self.create_conn_sqlalchemy()
        df = pd.read_sql(query, conn)
        return df

    def to_sql(self, df, name, schema='analyst', if_exists='fail', index=False, **kwargs):
        conn = self.create_conn_sqlalchemy()
        df.to_sql(name, conn, schema=schema, if_exists=if_exists, index=index, **kwargs)

    def read_table(self, name, columns='all', limit=None):
        """
        Read certain table
        :param name: table name
        :param columns: columns(s), list or string
        :param limit: limit of selected rows
        :return:
        """
        if columns == 'all':
            query = 'select * from %s' % name
        elif isinstance(columns, list):
            query = 'select %s from %s' % (','.join(columns), name)
        else:
            query = 'select %s from %s' % (columns, name)

        if limit is not None:
            query = query + ' limit %s ' % limit
        return self.read_sql(query)

    def show_columns(self, name):
        query = 'show columns from %s' % name
        return self.read_sql(query)

    def count(self, name):
        query = 'select count(*) from (%s) t' % name
        return self.read_sql(query)

    def show_tables(self, schema=None, like=''):
        if schema is None:
            schema = self.schema
        query = ''' SHOW TABLES from %s like '%s' ''' % (schema, like)
        return self.read_sql(query)

    def head(self, table_name, limit=5):
        """
        Show the head of certain table
        :param table_name: table name
        :param limit: limit
        :return:
        """
        query = ' select * from %s limit %s ' % (table_name, limit)
        return self.read_sql(query)


def test():
    db_unit = PrestoUnit(host='10.157.58.119', port=10000, username='diyang', password='Sephora123')
    db_unit.show_columns("dwd.v_users")


if __name__ == '__main__':
    # test()
    conn = create_engine(
        'hive://diyang@10.157.58.119:10000/dwd',
        connect_args={'auth': 'NOSASL'}
    )
    conn.execute('drop table if exists da_dev.iris_python_to_hive_test')
    df = pd.DataFrame({'id': [1, 2], 'account': ['ynn', 'wnn']})
    df.to_sql("iris_python_to_hive_test", conn, schema='da_dev', index=False, method='multi')