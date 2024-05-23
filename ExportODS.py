import pyodbc
from ODS import ODS


class ExportODS:
    buildSchema = """ 
DROP TABLE IF EXISTS OrderFact;
DROP TABLE IF EXISTS DimCustomer;
DROP TABLE IF EXISTS DimProduct;
DROP TABLE IF EXISTS DimCategory;
DROP TABLE IF EXISTS DimLocation;
DROP TABLE IF EXISTS DimDate;
DROP TABLE IF EXISTS DimCategoryLink;
DROP TABLE IF EXISTS DimOrderProductLink;

CREATE TABLE DimCustomer(
	CustomerID nvarchar(250) PRIMARY KEY,
	FirstName nvarchar(250),
	Surname nvarchar(250),
	CustomerType nvarchar(250)
)

CREATE TABLE DimProduct(
	ProductID nvarchar(150) PRIMARY KEY,
	ProductName nvarchar(500),
	ProductPrice money,
	Cost money,
	CategoryName nvarchar(50),
	Subcategory nvarchar(50)
)

CREATE TABLE DimCategory(
	CategoryName nvarchar(50) PRIMARY KEY,
	ParentCategory nvarchar(50)
)

CREATE TABLE DimLocation(
	LocationID nvarchar(150) PRIMARY KEY,
	Country nvarchar(150),
	City nvarchar(150),
	State nvarchar(150)
)

CREATE TABLE DimDate(
	DateID nvarchar(150) PRIMARY KEY,
	FullDate date,
	Day nvarchar(50),
	Month nvarchar(50),
	Year int,
	DayOfYear int,
	DayOfWeek int,
	Quarter int
)

CREATE TABLE DimCategoryLink(
	CategoryName nvarchar(50),
	OrderID nvarchar(150),
	CONSTRAINT pk_CategoryLink PRIMARY KEY(CategoryName, OrderID)
)

CREATE TABLE DimOrderProductLink(
	OrderID nvarchar(150),
	ProductID nvarchar(150),
	CONSTRAINT pk_OrderProductLink PRIMARY KEY(OrderID, ProductID)
)

CREATE TABLE OrderFact(
	OrderID nvarchar(150),
	CustomerID nvarchar(250),
	ProductID nvarchar(150),
	Quantity int,
	SaleAmount money,
	Cost money,
	LocationID nvarchar(150),
	DateID nvarchar(150)

	FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID),
	FOREIGN KEY (ProductID) REFERENCES DimProduct(ProductID),
	FOREIGN KEY (LocationID) REFERENCES DimLocation(LocationID),
	FOREIGN KEY (DateID) REFERENCES DimDate(DateID),
)
"""

    def __init__(self):
        print("Building SQL data warehouse from schema")
        connection_string = "DRIVER={SQL Server};SERVER=mssql.chester.network;DATABASE=db_2202901_data_analytics_warehouse;UID=user_db_2202901_data_analytics_warehouse;PWD=Testing123"
        self.conn = pyodbc.connect(connection_string)
        self.cursor = self.conn.cursor()

    def build_tables(self):
        print("\tBuilding ODS tables into SQL data warehouse tables")
        self.cursor.execute(self.buildSchema)
        self.conn.commit()
        print("done")

    def export_using_csv(self, table, dataframe, chunk_size):
        rows = len(dataframe.index)
        current = 0  # Starting at row 0
        while current < rows:
            if rows - current < chunk_size:  # Determine rows split into chunks
                stop = rows
            else:
                stop = current + chunk_size
            # Create a CSV file from current position to end of the chunk
            CSV = dataframe.iloc[current:stop].to_csv(index=False, header=False,
                                                      quoting=1, quotechar="'", lineterminator="),\n(")
            CSV = CSV[:-3]  # Delete last three chars i.e., ",\n"
            values = f"({CSV}"  # Convert CSV into a string
            # Write SQL string and replace empties with NULL
            SQL = f"INSERT INTO {table} VALUES {values}".replace("''", 'NULL')
            print(SQL)
            current = stop  # Move to end of chunk
            self.cursor.execute(SQL)  # Execute the SQL
        self.conn.commit()

    def export_ods(self):
        print("\tExporting ODS to SQL")
        chunk_size = 1
        for table in ODS.tables:
            df = getattr(ODS, f"{table}_df")
            print(f"\t Exporting to {table} from {table}_df with {len(df.index)} rows ")
            self.export_using_csv(table, df, chunk_size)

