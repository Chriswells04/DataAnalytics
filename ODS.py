import pandas as pd


class ODS:
    tables = [
        'DimCustomer',
        'DimProduct',
        'DimCategory',
        'DimLocation',
        'DimDate',
        'DimCategoryLink',
        'DimOrderProductLink',
        'OrderFact']

    # Changed customer to 4 columns as location relates to sale not customer
    DimCustomer_df = pd.DataFrame(columns=['CustomerID',
                                           'FirstName',
                                           'Surname',
                                           'CustomerType'])

    DimLocation_df = pd.DataFrame(columns=['LocationID', 'Country', 'City', 'State'])
    DimOrderProductLink_df = pd.DataFrame(columns=['OrderID', 'ProductID'])
    # Changed saleOfDate to FullDate name
    DimDate_df = pd.DataFrame(
        columns=['DateID', 'FullDate', 'Day', 'Month', 'Year', 'DayOfYear', 'DayOfWeek', 'Quarter'])
    # cost, categoryName, ParentCategory needs to be added to ERD, missed columns
    DimProduct_df = pd.DataFrame(
        columns=['ProductID', 'ProductName', 'ProductPrice', 'Cost', 'CategoryName', 'Subcategory'])
    DimCategory_df = pd.DataFrame(columns=['CategoryName', 'ParentCategory'])
    DimCategoryLink_df = pd.DataFrame(columns=['CategoryName', 'OrderID'])
    OrderFact_df = pd.DataFrame(
        columns=['OrderID', 'CustomerID', 'ProductID', 'Quantity', 'SaleAmount', 'Cost', 'LocationID', 'DateID'])
