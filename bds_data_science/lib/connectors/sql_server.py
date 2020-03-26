import pymssql
import pandas as pd
from sqlalchemy import create_engine

from bds_data_science.lib.common.df_ops import df_split


class SQLServerUnit:
    def __init__(self, host, port, username, password, db='default', charset='utf8'):
        host_port = host + ':' + port
        self.conn = pymssql.connect(
            host=host_port, user=username, password=password, database=db, charset=charset)
        self.engine = create_engine('mssql+pymssql://{username}:{password}@{servername}/{db}?charset={charset}'.format(
            username=username,
            password=password,
            servername=host_port,
            db=db, charset=charset),
            echo=True)

    def close_conn(self):
        try:
            if self.conn:
                self.conn.close()
        except pymssql.Error as e:
            print('Error: %s', e)

    def __execute_one(self, query):
        """
        :param query:  hive query
        :return:
        """
        cursor = self.conn.cursor()
        cursor.execute(query)

    def execute(self, query):
        """
        Execute hive

        :param query: hive query
        :return: None
        """
        queries = query.split(';')
        for q in queries:
            if q.strip():
                self.__execute_one(q)

    def drop_tab(self, tab_name: str):
        self.execute("drop table if exists %s" % tab_name)

    def get_df_from_db(self, query):
        """
        This function is going to read date from data base

        :param query: hive query
        :return: pandas data frame
        """
        cursor = self.conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        col_des = cursor.description
        col_des = [tuple([x[0].split('.')[1] if '.' in x[0] else x[0]] + list(x[1:])) for x in col_des]
        col_name = [col_des[i][0] for i in range(len(col_des))]
        ret_df = pd.DataFrame([list(i) for i in data], columns=col_name)
        return ret_df

    def _get_df_from_db(self, tab_name: str, cols: list or str = "*",
                        condition: str or None = None, limit: int or None = None):
        """
        Load df from db
        :param tab_name: table name
        :param cols: list of columns, if pass "*" will select all columns
        :param condition: selection condition
        :param limit: limit number
        :return:
        """
        cols = ', '.join(cols) if cols != '*' else cols
        sql_query = """SELECT {cols} FROM {tab} """.format(cols=cols, tab=tab_name)
        if condition:
            sql_query += """WHERE {cond} """.format(cond=condition)
        if limit:
            sql_query = """SELECT TOP {limit} {cols} FROM {tab} """.format(limit=limit,cols=cols,tab=tab_name)
        ret_df = pd.read_sql(sql_query, self.engine)
        return ret_df

    def df2db(self, df: pd.DataFrame, tab_name):
        """
        Upload a df to db
        :param df: df to upload
        :param tab_name: table name
        :return: None
        """

        self.execute("drop table if exists {table_name}".format(table_name=tab_name))
        df.to_sql(name=tab_name, con=self.engine, if_exists='append', index=False)

    def df2db_separate(self, df: pd.DataFrame, tab_name):
        """
        Upload a df to db in separate way
        :param df:
        :param tab_name: table name
        :return: None
        """
        self.execute("drop table if exists {table_name}".format(table_name=tab_name))
        max_df_size = 50

        dfs = df_split(df, batch_size=max_df_size)
        num_piece = len(dfs)

        dfs[0].to_sql(tab_name, self.engine, if_exists='append', index=False)
        if num_piece > 1:
            for pdf in dfs[1:]:
                self.execute("DROP TABLE IF EXISTS {tt}".format(tt=tab_name + '_tmp'))
                pdf.to_sql(tab_name + '_tmp', self.engine, if_exists='append', index=False)
                self.execute("INSERT INTO TABLE {tn} SELECT * FROM {tt}".format(
                    tn=tab_name, tt=tab_name + '_tmp'
                ))
                print(len(pdf))
            self.execute("DROP TABLE IF EXISTS {tt}".format(tt=tab_name + '_tmp'))


def test():
    sql_unit = SQLServerUnit(host='211.152.47.73', port='1433', username='iris.bao', password='irisbaoNIAN93',
                             db='Iris')
    sql_unit.execute(r"""
                            drop table if exists iris.dbo.iris_test
                            select top 100 * into iris.dbo.iris_test
                            from iris.dbo.TradeZoneStoreList
                            """)
    # df = sql_unit.get_df_from_db("select top 100 * from iris.dbo.iris_test")
    df = sql_unit._get_df_from_db(tab_name='iris.dbo.iris_test', limit=10)
    print(df)
    sql_unit.df2db(df=df, tab_name='sql_test')

    # sql_unit.df2db_separate(df=df, tab_name='sql_test')
    sql_unit.drop_tab(tab_name='dbo.sql_test')

    sql_unit.close_conn()


if __name__ == '__main__':
    test()
