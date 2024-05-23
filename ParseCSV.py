import pandas as pd
from ODS import ODS
from DateConfiguration import DateConfiguration


class ParseCSV:
    def __init__(self):
        print("Building Parse CSV through Constructor")
        self.orders_df = pd.read_csv("BuildCSV.csv")
        # print(self.orders_df.head())

    def parse_csv(self):
        print("Parsing csv files")
        self.parse_dates()
        self.parse_orders()
        self.parse_order_product_link()
        self.parse_location()
        self.parse_order_category_link()

    def parse_dates(self):
        print("\tParsing csv dates")
        dates_df = pd.DataFrame(self.orders_df['Order Date'])
        dates_df['FullDate'] = pd.to_datetime(dates_df['Order Date'], format='mixed')
        dates_df = DateConfiguration.config_date_values(self, dates_df)
        dates_df = dates_df.drop(columns=['Order Date'])
        ODS.DimDate_df = pd.concat([ODS.DimDate_df, dates_df])
        ODS.DimDate_df.drop_duplicates(['DateID'], keep='first', inplace=True)
        # print(ODS.DimDate_df.to_string())

    def parse_orders(self):
        print("\tParsing csv Orders")
        orders_df = pd.DataFrame(self.orders_df[['Order Date', 'Postal Code', 'Order ID',
        'Customer ID', 'Product ID', 'Sales', 'Quantity']])
        orders_df['FullDate'] = pd.to_datetime(orders_df['Order Date'], format='mixed')
        orders_df = DateConfiguration.config_date_values(self, orders_df)

        orders_df = pd.merge(orders_df, ODS.DimProduct_df[['ProductID', 'Cost']],
                             left_on='Product ID', right_on='ProductID', how='left')
        orders_df = orders_df.drop(columns=['Order Date', 'Day', 'Month', 'Year',
                                            'DayOfWeek', 'DayOfYear', 'Quarter', 'FullDate', 'Product ID'])
        orders_df = orders_df.rename(columns={'Postal Code': 'LocationID', 'Order ID': 'OrderID',
                                              'Sales': 'SaleAmount', 'Customer ID': 'CustomerID'})
        ODS.OrderFact_df = pd.concat((ODS.OrderFact_df, orders_df))
        ODS.OrderFact_df.drop_duplicates(['OrderID'], keep='first', inplace=True)
        # print(ODS.FactOrder_df.to_string())

    def parse_order_product_link(self):
        print('\tParsing csv Order Product Link')
        order_product_df = pd.DataFrame(self.orders_df[['Order ID', 'Product ID']])
        order_product_df = order_product_df.rename(columns={'Order ID': 'OrderID', 'Product ID': 'ProductID'})
        ODS.DimOrderProductLink_df = pd.concat((ODS.DimOrderProductLink_df, order_product_df))
        ODS.DimOrderProductLink_df.drop_duplicates(['OrderID'], keep='first', inplace=True)
        # print(ODS.OrderProductLink_df.to_string())

    def parse_location(self):
        print('\tParsing csv location')
        location_df = pd.DataFrame(self.orders_df[['City', 'State', 'Country', 'Postal Code']])
        location_df = location_df.rename(columns={'Postal Code': 'LocationID'})
        ODS.DimLocation_df = pd.concat((ODS.DimLocation_df, location_df))
        ODS.DimLocation_df.drop_duplicates(['LocationID'], keep='first', inplace=True)
        # print(ODS.DimLocation_df .to_string())

    def parse_order_category_link(self):
        print('\tParsing csv order category link')
        order_category_df = pd.DataFrame(self.orders_df[['Sub-Category', 'Order ID']])
        order_category_df = order_category_df.rename(columns={'Sub-Category': 'CategoryName', 'Order ID': 'OrderID'})
        ODS.DimCategoryLink_df = pd.concat((ODS.DimCategoryLink_df, order_category_df))
        ODS.DimCategoryLink_df.drop_duplicates(['OrderID'], keep='first', inplace=True)
        # print(ODS.DimCategoryLink_df.to_string())
