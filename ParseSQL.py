import pyodbc
import pandas as pd
from ODS import ODS
from DateConfiguration import DateConfiguration


class ParseSQL:
    def __init__(self):
        print("Building")
        connection_string = "DRIVER={SQL Server};SERVER=mssql.chester.network;DATABASE=db_2202901_data_analytics_assignment2;UID=user_db_2202901_data_analytics_assignment2;PWD=Testing123"
        self.conn = pyodbc.connect(connection_string)
        self.cursor = self.conn.cursor()

    def parse_sql(self):
        print("parsing SQL")
        self.parse_dates()
        self.parse_location()
        self.parse_product()
        self.parse_customer()
        self.parse_category()
        self.parse_order()
        self.match_category_to_order()
        self.match_product_to_order()

    def parse_dates(self):
        print("\tparsing SQL dates")
        dates_df = pd.read_sql_query("SELECT DateOfSale FROM Sale", self.conn)
        dates_df['FullDate'] = pd.to_datetime(dates_df['DateOfSale'])
        dates_df = DateConfiguration.config_date_values(self, dates_df)

        dates_df = dates_df.drop(columns=['DateOfSale'])
        # As of pandas 2.0 and above append is deprecated and concat is used instead
        # ODS.DimDate_df = ODS.DimDate_df.append(dates_df)
        ODS.DimDate_df = pd.concat([ODS.DimDate_df, dates_df])
        # ODS.DimDate_df.drop(subset='DateID', keep='first', inplace=True)
        ODS.DimDate_df.drop_duplicates(['DateID'], keep='first', inplace=True)
        # print(dates_df.head())
        # print(ODS.DimDate_df.head())

    def parse_location(self):
        print("\tparse location")
        locations_df = pd.read_sql_query("SELECT PostalCode as LocationID, City, State, Country FROM Sale", self.conn)
        ODS.DimLocation_df = pd.concat([ODS.DimLocation_df, locations_df])
        ODS.DimLocation_df.drop_duplicates(['LocationID'], keep='first', inplace=True)
        # print(ODS.DimLocation_df.head())

    def parse_customer(self):
        print("\tparse customers")
        customers_df = pd.read_sql_query("SELECT * FROM Customer", self.conn)
        ODS.DimCustomer_df = pd.concat([ODS.DimCustomer_df, customers_df])
        # print(customers_df.head())
        # print(ODS.DimCustomer_df.head())

    def parse_product(self):
        print("\tparse product")
        products_df = pd.read_sql_query(
            "SELECT ProductID, ProductName, ProductPrice, Cost, Subcategory, Category as CategoryName From Product",
            self.conn)

        # print(products_df.head())

        ODS.DimProduct_df = pd.concat([ODS.DimProduct_df, products_df])

        # potentially unnecessary as all products will be unique
        ODS.DimProduct_df.drop_duplicates(['ProductID'], keep='first', inplace=True)
        # print(ODS.DimProduct_df.head())

    def parse_category(self):
        print("\tparse category")
        category_df = pd.read_sql_query("SELECT * FROM Category", self.conn)
        ODS.DimCategory_df = pd.concat([ODS.DimCategory_df, category_df])
        # print(ODS.DimCategory_df.head())

    def parse_order(self):
        print("\tparse Order")
        order_df = pd.read_sql_query(
            "SELECT OrderID, DateOfSale, SaleAmount, CustomerID, PostalCode as LocationID From Sale",
            self.conn)
        order_df['DateOfSale'] = pd.to_datetime(order_df['DateOfSale'])
        order_df['DateID'] = order_df['DateOfSale'].dt.strftime('%Y%m%d')
        order_df = order_df.drop(columns=['DateOfSale'])

        sale_item_df = pd.read_sql_query("SELECT * FROM SaleItem", self.conn)
        order_df = pd.merge(order_df, sale_item_df[['OrderID', 'ProductID', 'Quantity']], left_on='OrderID',
                            right_on='OrderID', how='left')
        order_df = pd.merge(order_df, ODS.DimProduct_df[['ProductID', 'Cost']], left_on='ProductID',
                            right_on='ProductID', how='left')
        ODS.OrderFact_df = pd.concat([ODS.OrderFact_df, order_df])
        # print(ODS.FactOrder_df)

    def match_category_to_order(self):
        print("\tmatching category to order")
        category_order_df = pd.merge(ODS.DimProduct_df[['ProductID', 'CategoryName']],
                                     ODS.OrderFact_df[['ProductID', 'OrderID']], left_on='ProductID',
                                     right_on='ProductID', how='left')
        category_order_df = category_order_df.drop(columns=['ProductID'])
        ODS.DimCategoryLink_df = pd.concat([ODS.DimCategoryLink_df, category_order_df])
        # print(ODS.DimCategoryLink_df.head())

    def match_product_to_order(self):
        print("\tmatching product to order")
        product_order_df = pd.merge(ODS.DimProduct_df[['ProductID']], ODS.OrderFact_df[['OrderID', 'ProductID']],
                                    left_on='ProductID', right_on='ProductID', how='left')
        ODS.DimOrderProductLink_df = pd.concat([ODS.DimOrderProductLink_df, product_order_df])
        # print(ODS.OrderProductLink_df.to_string())

    # done
