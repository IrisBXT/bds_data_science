import pandas as pd

from bds_data_science.lib.connectors.sql_server import SQLServerUnit

if __name__ == '__main__':
    sql_unit = SQLServerUnit(host='211.152.47.73', port='1433', username='iris.bao', password='irisbaoNIAN93',
                             db='Iris')
    data_path = "D:/WorkSpace/BDScases/BK/sales_driver/"
    sales_data = pd.read_csv("%ssales.csv" % data_path, encoding='gbk')
    print(sales_data)
    holiday_list = pd.read_csv("%sholiday_list.csv" % data_path)
    print(holiday_list)
    sql_unit.df2db(sales_data, tab_name='bk_salesdriver_0415')
    sql_unit.df2db(holiday_list, tab_name='holiday_list')
    sql_unit.release()
