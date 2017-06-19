"""
Connecting and retrieving data from MS Access

Special guest(s):
    pyodbc
    https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-Microsoft-Access

"""

import pyodbc
import pandas



class MSAccessDb:
    """
    Handler to an MS Access database file and its contents.
    """

    def __init__(self, file_path):
        """
        Initializes MsAccessDb as a handler to the MS Access database file.
        """
        driver_list = [driver for driver in pyodbc.drivers()
                       if driver.startswith('Microsoft Access Driver')]
        if driver_list is None:
            raise pyodbc.OperationalError

        self.access_file_path = file_path
        self.connection_str = ("Driver={Microsoft Access Driver (*.mdb, *.accdb)}; "
                               "DBQ=" + self.access_file_path)
        self.connection = pyodbc.connect(self.connection_str)
        self.tbl_list = []


    def get_table_list(self, table_type='TABLE', table_names_only=True):
        """
        Returns a list of tables as pyodbc.row objects and their metadata,
        if table_names_only is True (default), ignores the other metadata
        returned.
        """
        self.tbl_list.clear()   # Clear state if called again
        cursor = self.connection.cursor()
        for row in cursor.tables(tableType=table_type):
            if table_names_only is True:
                self.tbl_list.append(row[2])
            else:
                self.tbl_list.append(row)
        cursor.close()

        return self.tbl_list


    def fetch_table(self, table_name):
        """
        Takes a table name and returns a list of all the data in that table,
        each record is a tuple
        """
        sql_query = "SELECT * FROM " + table_name
        cursor = self.connection.cursor()
        results = cursor.execute(sql_query).fetchall()
        cursor.close()

        return results


    def to_pandas_dataframe(self, table_name):
        """
        Takes a table name, selects all data from that table and returns a
        pandas dataframe of that data
        """
        sql = "SELECT * FROM " + table_name
        dataframe = pandas.read_sql(sql, self.connection)

        return dataframe


if __name__ == '__main__':

    # USAGE:
    # help(MSAccessDb)

    # Initialize a new MsAccessDb object
    ms_access_db = MSAccessDb(r'C:\Users\Erik\Desktop\IMPDB_to_SQL_TestDir\2015_536-Sector_Model_00000_United_States_US.impdb')

    # Table list
    table_list = ms_access_db.get_table_list()
    record_attr_list = []
    for table in table_list:
        ms_access_db.fetch_table_columns(table, ['column_name'])
