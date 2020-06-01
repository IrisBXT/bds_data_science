import pandas as pd

from lib.connectors.sql_server import SQLServerUnit

class EnviDes:
    def __init__(self, sql_unit: SQLServerUnit, brand_name: str, year_month: list, cities: list, envi_range: float):
        self.sql_unit = sql_unit
        self.brand_name = brand_name
        self.year_month = '\',\''.join(year_month)
        self.cities = '\',\''.join(cities)
        self.envi_range = envi_range

    @property
    def _default_col(self):
        return {
            '[BurgerKing].[dbo].[BKStoreList]': {
                'store_name': 'BDSStoreName',
                'lat': 'StoreLat', 'lng': 'StoreLng', 'city': 'City'},
            '[MCDDecisionSupport].[dbo].[MCDStoreList]': {
                'store_name': 'BDSStoreName',
                'lat': 'StoreLat', 'lng': 'StoreLng', 'city': 'City'},
            '[Yum].[dbo].[DeliveryStoreList]': {
                'store_name': 'StoreName',
                'lat': 'Latitude', 'lng': 'Longitude', 'city': 'City',
                'brand': 'Brand', 'category': 'Category'},
            '[Yum].[dbo].[TotDeliveryRestaurantList]': {
                'store_name': 'StoreName',
                'lat': 'Lat', 'lng': 'Lon', 'city': 'City',
                'brand': 'BrandName', 'category': 'Category'},
            '[Yum].[dbo].[ResultMonthlyStoreSales]': {
                'store_name': 'RestaurantName',
                'yearmonth': 'YearMonth', 'channel': 'ChannelName', 'city': 'City',
                'brand': 'BrandName', 'category': 'RestaurantCategoryName',
                'sales': 'ProductSales', 'order_cnt': 'OrderCount'},
            '[Yum].[dbo].[TotResultMonthlyStoreSales]': {
                'store_name': 'RestaurantName',
                'yearmonth': 'YearMonth', 'channel': 'ChannelName', 'city': 'City',
                'brand': 'BrandName', 'category': 'RestaurantCategoryName',
                'sales': 'ProductSales', 'order_cnt': 'OrderCount'}
        }

    def code_format(self, code_base: str,
                    cate_select: str, cate_select_s: str,
                    store_list: str, store_sales: str, cmb: str,
                    store_list_brand: str):
        print(code_base)
        format_code = code_base.format(store_list=store_list, store_sales=store_sales,
                                       store_list_brand=store_list_brand,
                                       cate_selection=cate_select,
                                       cate_selection_s=cate_select_s,
                                       store_name_brand=self._default_col[store_list_brand]['store_name'],
                                       lat_brand=self._default_col[store_list_brand]['lat'],
                                       lng_brand=self._default_col[store_list_brand]['lng'],
                                       city_brand=self._default_col[store_list_brand]['city'],
                                       store_name=self._default_col[store_list]['store_name'],
                                       brand_name=self.brand_name,
                                       brand=self._default_col[store_list]['brand'],
                                       lat=self._default_col[store_list]['lat'],
                                       lng=self._default_col[store_list]['lng'],
                                       city=self._default_col[store_list]['city'],
                                       cities=self.cities,
                                       city_s=self._default_col[store_sales]['city'],
                                       store_name_s=self._default_col[store_sales]['store_name'],
                                       channel_s=self._default_col[store_sales]['channel'],
                                       order_cnt=self._default_col[store_sales]['order_cnt'],
                                       sales=self._default_col[store_sales]['sales'],
                                       yearmonth=self._default_col[store_sales]['yearmonth'],
                                       year_month=self.year_month,
                                       envi_range=self.envi_range,
                                       cmb=cmb)
        return format_code

    def sales_des(self, cates: str, brands=None, if_avg=False):
        """
        Combine information of sales of stores around target brand
        :param cates: the list of store type :['total'] for all, ['wqsr'] for WQSR etc.
        :param brands: if cates=='keybrand' then use brands to select stores
        :param if_avg: if True the function will return average sales values
        :return: data frame of combined information
        """

        store_list = '[Yum].[dbo].[DeliveryStoreList]'
        store_sales = '[Yum].[dbo].[ResultMonthlyStoreSales]'
        if self.brand_name == '汉堡王':
            store_list_brand = '[BurgerKing].[dbo].[BKStoreList]'
        elif self.brand_name == '麦当劳':
            store_list_brand = '[MCDDecisionSupport].[dbo].[MCDStoreList]'
        else:
            store_list_brand = store_list

        if if_avg:
            cmb = 'avg'
        else:
            cmb = 'sum'

        code_base = r"""
        select tttt1.store_name,tttt2.channel,count(distinct tttt1.RestaurantName) as store_cnt,
        sum(tttt2.envi_order_cnt) as envi_order_cnt,sum(tttt2.envi_sales) as envi_sales
        from
        (
            select ttt1.store_name,'{brand_name}' as brand,ttt2.store_name as RestaurantName,
            [yum].[dbo].[fnCalcDistanceKM](ttt1.lat,ttt1.lng,ttt2.lat,ttt2.lng) as distance
            from 
            (
                select  tt1.store_name, tt1.lat, tt2.lng
                from (
                    select  {store_name_brand} as store_name,{lat_brand} as lat,
                    row_number() over (partition by {store_name_brand} order by lat_cnt desc) rn
                    from
                    (
                        select {store_name_brand},{lat_brand},count(0) as lat_cnt
                        from {store_list_brand}
                        where {city_brand} in ('{cities}')
                        and {lat_brand} is not null and {store_name_brand} is not null and {store_name_brand}<>''
                        group by {store_name_brand},{lat_brand}
                        )t1)tt1 left outer join
                    (
                    select  {store_name_brand} as store_name, {lng_brand} as lng,
                    row_number() over (partition by {store_name_brand} order by lng_cnt desc) rn
                    from
                    (
                        select {store_name_brand},{lng_brand},count(0) as lng_cnt
                        from {store_list_brand}
                        where {city_brand} in ('{cities}')
                        and {lng_brand} is not null and {store_name_brand} is not null and {store_name_brand}<>''
                        group by {store_name_brand},{lng_brand}
                        )t1)tt2 on tt1.store_name=tt2.store_name
                where tt1.rn=1 and tt2.rn=1
            )ttt1 cross join 
            (
                select  tt1.store_name, tt1.lat, tt2.lng, tt1.brand
                from (
                    select  {store_name} as store_name,{brand} as brand,{lat} as lat,
                    row_number() over (partition by {store_name} order by lat_cnt desc) rn
                    from
                    (
                        select {store_name},{brand},{lat},count(0) as lat_cnt
                        from {store_list}
                        where {city} in ('{cities}')
                        {cate_selection}
                        and {lat} is not null and {store_name} is not null and {store_name}<>''                      
                        group by {store_name},{brand},{lat}
                        )t1)tt1 left outer join
                    (
                    select  {store_name} as store_name, {lng} as lng,
                    row_number() over (partition by {store_name} order by lng_cnt desc) rn
                    from
                    (
                        select {store_name},{lng},count(0) as lng_cnt
                        from {store_list}
                        where {city} in ('{cities}')
                        {cate_selection}
                        and {lng} is not null and {store_name} is not null and {store_name}<>''  
                        group by {store_name},{lng}
                        )t1)tt2 on tt1.store_name=tt2.store_name
                where tt1.rn=1 and tt2.rn=1
            )ttt2)tttt1 left outer join
            (
            SELECT {store_name_s} as RestaurantName,upper({channel_s}) as channel,
            {cmb}({order_cnt}) as envi_order_cnt,{cmb}({sales}) as envi_sales
            FROM {store_sales}    
            where {city_s} in ('{cities}')
            and {yearmonth} in ('{year_month}')
            {cate_selection_s}
            and upper({channel_s}) in ('ELEME','MEITUAN')
            and {store_name_s} is not null and {store_name_s}<>'' 
            group by {store_name_s},upper({channel_s})
            )tttt2 on tttt1.RestaurantName=tttt2.RestaurantName
        where tttt1.distance<={envi_range}  and tttt2.channel is not null
        group by store_name,channel
        order by channel,store_name         
        """

        if cates == 'keybrand':
            brands = '\',\''.join(brands)
            cate_select = "and {brand} in ('{brands}')".format(brand=self._default_col[store_list]['brand'],
                                                               brands=brands)
            cate_select_s = "and {brand_s} in ('{brands}')".format(brand_s=self._default_col[store_sales]['brand'],
                                                                   brands=brands)
            format_code = self.code_format(code_base=code_base,
                                           cate_select=cate_select,
                                           cate_select_s=cate_select_s,
                                           store_list=store_list,
                                           store_sales=store_sales,
                                           store_list_brand=store_list_brand,
                                           cmb=cmb)
            print(format_code)
            envi_sales_df = self.sql_unit.get_df_from_db(format_code)
        elif cates == 'wqsr':
            cate_select = "and {category}='WQSR'".format(category=self._default_col[store_list]['category'])
            cate_select_s = "and {category_s}='WQSR'".format(category_s=self._default_col[store_sales]['category'])
            format_code = self.code_format(code_base=code_base,
                                           cate_select=cate_select,
                                           cate_select_s=cate_select_s,
                                           store_list=store_list,
                                           store_sales=store_sales,
                                           store_list_brand=store_list_brand,
                                           cmb=cmb)
            print(format_code)
            envi_sales_df = self.sql_unit.get_df_from_db(format_code)
        elif cates == 'total':
            store_list = '[Yum].[dbo].[TotDeliveryRestaurantList]'
            store_sales = '[Yum].[dbo].[TotResultMonthlyStoreSales]'
            format_code = self.code_format(code_base=code_base,
                                           cate_select='',
                                           store_list=store_list,
                                           cate_select_s='',
                                           store_list_brand=store_list_brand,
                                           store_sales=store_sales, cmb=cmb)
            print(format_code)
            envi_sales_df = self.sql_unit.get_df_from_db(format_code)
        else:
            print('ERROR: Wrong type of category!')
            envi_sales_df = None
        sales_df = self.sql_unit.get_df_from_db(r"""
            select {store_name} as store_name,upper({channel}) as channel,
            {cmb}({oreder_cnt}) as order_cnt,{cmb}({sales}) as sales
            from {store_sales}
            where {city} in ('{cities}')
                and {brand}='{brand_name}'
                and {yearmonth} in ('{year_month}')
                and {store_name} is not null and {store_name}<>''
                and upper({channel}) in ('ELEME','MEITUAN')
            group by {store_name},upper({channel})
            order by channel, store_name  
            """.format(store_name=self._default_col[store_sales]['store_name'],
                       channel=self._default_col[store_sales]['channel'],
                       oreder_cnt=self._default_col[store_sales]['order_cnt'],
                       sales=self._default_col[store_sales]['sales'],
                       city=self._default_col[store_sales]['city'],
                       brand=self._default_col[store_sales]['brand'],
                       yearmonth=self._default_col[store_sales]['yearmonth'],
                       store_sales=store_sales, cmb=cmb,
                       cities=self.cities, brand_name=self.brand_name, year_month=self.year_month
                       ))
        sales_df = pd.merge(sales_df, envi_sales_df, how='outer')
        return sales_df

    def area_des(self, build_type: list):
        pass


if __name__ == '__main__':
    sql_unit = SQLServerUnit(host='211.152.47.66', port='1433', username='iris.bao', password='irisbaoNIAN93',
                             db='MCD')
    envi = EnviDes(sql_unit=sql_unit, brand_name='汉堡王', year_month=['202004'], cities=['上海'], envi_range=3)
    df = envi.sales_des(cates='wqsr', if_avg=False)
    # df = envi.sales_des(cates='keybrand',brands=['麦当劳', '肯德基'], if_avg=False)
    print(df)
    df.to_csv("D:/workspace/Python/sales_df.csv", index=False, encoding='utf_8_sig')
    # df = pd.read_csv("D:/workspace/Python/sales_df.csv", encoding='utf-8')
    # df.to_csv("D:/workspace/Python/sales_df.csv", index=False, encoding='utf_8_sig')
    sql_unit.release()
