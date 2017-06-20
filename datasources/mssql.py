"""
Connecting to MS SQL Server

Special guest(s):
    sqlalchemy: http://docs.sqlalchemy.org/en/rel_1_1/dialects/mssql.html
    pyodbc:

"""
import pandas
from sqlalchemy import create_engine
from urllib import parse
import pyodbc

class MSSqlDb:
    """
    Handler to an MS SQL Server database and its contents
    """

    def __init__(self, connection_dict):
        """ Initializes SQLAlchemy engine, using pyodbc as the DBAPI and the connection dictionary provided"""
        self.connection_dict = connection_dict
        self.engine = create_engine("mssql+pyodbc:///?odbc_connect={0}".format(self.dict_to_url()))


    def dict_to_url(self):
        connection_string = ("DRIVER={0};"
                             "SERVER={1};"
                             "DATABASE={2};"
                             "Trusted_Connection={3}".
                             format(self.connection_dict['DRIVER'],
                                    self.connection_dict['SERVER'],
                                    self.connection_dict['DATABASE'],
                                    self.connection_dict['Trusted_Connection']))
        connection_url = parse.quote_plus(connection_string)
        return connection_url


    def bulk_insert(self, csv_file_path, sql_schema_name, sql_table_name,
                    field_terminator, row_terminator):
        """
        https://docs.microsoft.com/en-us/sql/t-sql/statements/bulk-insert-transact-sql

        EXAMPLE:

        MSSqlDb.bulk_insert(self,
                    csv_file_path=r'C:\mycsv.csv',
                    sql_schema_name='dbo'
                    sql_table_name='mytable',
                    field_terminator=',',
                    row_terminator='\\n'
                    )
        """
        sql = ("BULK INSERT [{0}].[{1}]\n"
               "FROM '{2}'\n"
               "WITH (FIELDTERMINATOR = '{3}', ROWTERMINATOR = '{4}');".format(
                   sql_schema_name,
                   sql_table_name,
                   csv_file_path,
                   field_terminator,
                   row_terminator
               ))

        # This is the only implementation that works at the moment in
        # issuing and committing the BULK INSERT functionality successfully
        # in SQL Server (2016)
        raw_con = self.engine.raw_connection()
        cursor = raw_con.cursor()
        cursor.execute(sql)
        cursor.commit()
        cursor.close()


if __name__ == '__main__':
    """
    Usage
    """
