import json
import pandas as pd
from DateConfiguration import DateConfiguration
from ODS import ODS


class ParseJSON:
    def __init__(self):
        print("Building Parse JSON through Constructor")

        # since Python 3.9 encoding parameter has been deprecated
        with open("BuildJSON.json") as f:
            self.data = json.load(f)

        self.order_df = pd.json_normalize(self.data['Sales'])
        # self.order_items_df = pd.json_normalize(self.order_df['Items'])

    def parse_json(self):
        print("Parsing JSON file")
        self.parse_dates()
        self.parse_location()
        self.parse_orders()
        self.parse_order_product_link()
        self.parse_order_category_link()
        # no reference to categorise through JSON, therefore no new data to be inserted

    def parse_dates(self):
        print("\tParsing JSON dates")
        dates_df = pd.DataFrame(self.order_df['Order Date'])
        dates_df['FullDate'] = pd.to_datetime(dates_df['Order Date'], format='mixed')
        dates_df = DateConfiguration.config_date_values(self, dates_df)
        dates_df = dates_df.drop(columns=['Order Date'])
        ODS.DimDate_df = pd.concat([ODS.DimDate_df, dates_df])
        ODS.DimDate_df.drop_duplicates(['DateID'], keep='first', inplace=True)
        # print(ODS.DimDate_df.to_string())

    def parse_location(self):
        print("\tParsing JSON location")
        locations_df = pd.DataFrame(self.order_df[['Postal Code', 'Country', 'State', 'City']])
        locations_df = locations_df.rename(columns={'Postal Code': 'LocationID'})
        ODS.DimLocation_df = pd.concat([ODS.DimLocation_df, locations_df])
        ODS.DimLocation_df.drop_duplicates(['LocationID'], keep='first', inplace=True)
        # print(ODS.DimLocation_df.to_string())

    def parse_orders(self):
        print("\tParsing JSON orders")
        order_items_df = pd.json_normalize(self.data['Sales'], record_path='Items', meta=['Order ID', 'Customer ID',
                                                                                          'Postal Code', 'Order Date'])
        dates_df = pd.DataFrame(order_items_df['Order Date'])
        dates_df['FullDate'] = pd.to_datetime(dates_df['Order Date'], format='mixed')
        dates_df = DateConfiguration.config_date_values(self, dates_df)
        order_items_df = pd.merge(order_items_df, dates_df[['Order Date', 'DateID']],
                                  left_on='Order Date', right_on='Order Date', how='left')
        order_items_df = order_items_df.drop(columns=['Order Date'])
        order_items_df = order_items_df.rename(columns={'Product ID': 'ProductID',
                                                        'Order ID': 'OrderID',
                                                        'Sales': 'SaleAmount',
                                                        'Postal Code': 'LocationID', 'Customer ID': 'CustomerID'})
        order_items_df = pd.merge(order_items_df, ODS.DimProduct_df[['ProductID', 'Cost']])
        ODS.OrderFact_df = pd.concat([ODS.OrderFact_df, order_items_df])
        ODS.OrderFact_df.drop_duplicates(['OrderID'], keep='first', inplace=True)
        # print(ODS.FactOrder_df.to_string())

    def parse_order_product_link(self):
        print('\tParsing JSON order products link')
        order_product_df = pd.json_normalize(self.data['Sales'], record_path='Items', meta=['Order ID'])
        order_product_df = order_product_df.drop(columns=['Quantity', 'Sales'])
        order_product_df = order_product_df.rename(columns={'Order ID': 'OrderID', 'Product ID': 'ProductID'})
        ODS.DimOrderProductLink_df = pd.concat([ODS.DimOrderProductLink_df, order_product_df])

        ODS.DimOrderProductLink_df.dropna(subset=['OrderID'], inplace=True)
        ODS.DimOrderProductLink_df.drop_duplicates(['OrderID'], keep='first', inplace=True)
        # print(ODS.OrderProductLink_df.to_string())

    def parse_order_category_link(self):
        print('\tParsing JSON order category link')
        order_category_df = pd.json_normalize(self.data['Sales'], record_path='Items', meta=['Order ID'])
        order_category_df = pd.merge(order_category_df, ODS.DimProduct_df[['ProductID', 'CategoryName']],
                                     left_on='Product ID', right_on='ProductID', how='left')
        order_category_df = order_category_df.drop(columns=['Product ID', 'Quantity', 'ProductID', 'Sales'])
        order_category_df = order_category_df.rename(columns={'Order ID': 'OrderID'})
        ODS.DimCategoryLink_df = pd.concat([ODS.DimCategoryLink_df, order_category_df])
        ODS.DimCategoryLink_df.drop_duplicates(['OrderID'], keep='first', inplace=True)
        ODS.DimCategoryLink_df.dropna(subset=['OrderID'], inplace=True)

        # null_mask = ODS.DimCategoryLink_df.isnull().any(axis=1)
        # null_rows = ODS.DimCategoryLink_df[null_mask]

        # print(null_rows)
        # print(ODS.DimCategoryLink_df.to_string())
