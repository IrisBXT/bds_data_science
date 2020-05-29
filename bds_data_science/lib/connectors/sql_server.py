import pymssql
import pandas as pd
from sqlalchemy import create_engine



class SQLServerUnit:
    def __init__(self, host, port, username, password, db, charset='utf8'):
        host_port = host + ':' + port
        self.conn = pymssql.connect(
            host=host_port, user=username, password=password, database=db, charset=charset)
        self.engine = create_engine('mssql+pymssql://{username}:{password}@{servername}/{db}?charset={charset}'.format(
            username=username,
            password=password,
            servername=host_port,
            db=db, charset=charset),
            echo=True)

    def release(self):
        try:
            if self.conn:
                self.conn.close()
            if self.engine:
                self.engine.dispose()
        except pymssql.Error as e:
            print('Error: %s', e)

    def __execute_one(self, query):
        """
        :param query:  hive query
        :return:
        """
        self.engine.execute(query)

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
        print('TABLE DROPPED!')

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

    def df2db(self, df: pd.DataFrame, tab_name, append=False):
        """
        Upload a df to db
        :param append:
        :param df: df to upload
        :param tab_name: table name
        :return: None
        """
        if append:
            df.to_sql(name=tab_name, con=self.engine, if_exists='append', index=False)
        else:
            self.execute("drop table if exists {table_name}".format(table_name=tab_name))
            df.to_sql(name=tab_name, con=self.engine, if_exists='fail', index=False)

    def create_table(self, tab_name: str, cols: dict):
        """
        :param tab_name:
        :param cols:
        :return:
        """
        col_type = []
        for k, v in cols.items():
            col_type.append(k + ' ' + v)
        col_type = ','.join(col_type)
        self.execute("""
                        DROP TABLE IF EXISTS {tab_name}
                        CREATE TABLE {tab_name} ({col_type})""".format(tab_name=tab_name, col_type=col_type))
        print('CREATE TABLE SUCCESS!')


def test():
    sql_unit = SQLServerUnit(host='211.152.47.73', port='1433', username='iris.bao', password='irisbaoNIAN93',
                             db='Iris')
    # sql_unit.create_table(tab_name='iris.dbo.create_test', cols={'A': 'nvarchar(255)', 'B': 'int'})
    sql_unit.execute(r"""
                        drop table if exists iris.dbo.sql_execute_test;
                        create table iris.dbo.sql_execute_test
                        (
                        [MCDStoreID] int not null,
                        [BDSStoreName] nvarchar(255) null
                        );
                        insert into iris.dbo.sql_execute_test
                        select top 100 MCDStoreID,BDSStoreName
                        from iris.dbo.TradeZoneStoreList
                        """)
    # df = sql_unit.get_df_from_db("select top 100 * from iris.dbo.sql_execute_test")
    # print(df)
    # sql_unit.df2db(df=df, tab_name='sql_test_df2db', append=False)
    df = sql_unit.get_df_from_db(r"""select MCDStoreID, convert(nvarchar(255),BDSSTORENAME) AS storename 
                                    from iris.dbo.sql_test_df2db""")
    print(df)
    # sql_unit.drop_tab(tab_name='iris.dbo.create_test')
    sql_unit.release()


if __name__ == '__main__':
    test()
