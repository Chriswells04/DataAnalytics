from ParseSQL import ParseSQL
from ParseCSV import ParseCSV
from ParseJSON import ParseJSON
from ExportODS import ExportODS


class OrderMain:
    def __init__(self):
        sql_parser = ParseSQL()
        csv_parser = ParseCSV()
        json_parser = ParseJSON()
        export = ExportODS()

        sql_parser.parse_sql()
        csv_parser.parse_csv()
        json_parser.parse_json()
        # export.build_tables()
        # export.export_ods()


main = OrderMain()
