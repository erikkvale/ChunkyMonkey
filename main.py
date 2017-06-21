import csv
import os
import tempfile
import pandas as pd
from datasources.mssql import MSSqlDb
from pandas.errors import ParserError


def chunky_monkey_csv_loader(csv_file_path,
                             sqlalchemy_engine,
                             sql_schema_name,
                             sql_table_name,
                             bulk_insert=True,
                             included_columns=None,
                             chunk_size=10000):
    """
    Reads the specified csv file by chunks, each chunk is a Pandas dataframe
    and is loaded in to the specified SQL database by each unit chunk. This
    chunking is specified in the documentation for the Pandas.Dataframe.read_csv()
    method and the iterator object returned when 'chunksize' is specified. If
    bulk_insert=True, then utilize the SQL flavor's bulk insert method if available.
    If bulk_insert=False, then use the Pandas.Dataframe.to_sql() method; although,
    depending on the DBAPI used in the SQLAlchemy connection, this could be very
    slow, so bulk_insert=True is the default.
    """

    chunk_iterator = pd.read_csv(filepath_or_buffer=csv_file_path,
                                 usecols=included_columns,
                                 iterator=True,
                                 chunksize=chunk_size)
    for chunk in chunk_iterator:

        if bulk_insert:
            try:
                # delete=False and manual close and delete for Windows NT:
                # https://docs.python.org/3/library/tempfile.html
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_csv:
                    chunk.to_csv(temp_csv.name, header=False, columns=included_columns)
                    temp_csv.close()
                    sql_db.bulk_insert(csv_file_path=temp_csv.name,
                                       sql_schema_name=sql_schema_name,
                                       sql_table_name=sql_table_name,
                                       field_terminator=',',
                                       row_terminator='\\n')
            except Exception as e:
                print(e)
                print(chunk)
            except ParserError as p:
                print('Check that the file is not malformed')
            finally:
                os.unlink(temp_csv.name)
                print('A monkey has chunked again')
        else:
            chunk.to_sql(sql_table_name, sql_db.engine, sql_schema_name, if_exists='append')


if __name__=='__main__':

    big_csv_file = r'C:\Users\Erik\Documents\University\CNR-PAG_Raju\data\data\COND.csv'

    conn_dict = {
        'DRIVER': 'ODBC Driver 13 for SQL Server',
        'SERVER': 'AURVANDIL\SQLEXPRESS',
        'DATABASE': 'Data_Raju',
        'Trusted_Connection': 'yes'
    }

    sql_db = MSSqlDb(connection_dict=conn_dict)

    # TREE.csv:
    # columns = [
    #      'PLT_CN',
    #      'INVYR',
    #      'CONDID',
    #      'STATECD',
    #      'COUNTYCD',
    #      'CYCLE',
    #      'PLOT',
    #      'TPA_UNADJ',
    #      'DRYBIO_BOLE',
    #      'DRYBIO_STUMP',
    #      'DRYBIO_TOP',
    #      'DRYBIO_SAPLING',
    #      'DRYBIO_WDLD_SPP',
    #      'DRYBIO_AG',
    #      'DRYBIO_BG'
    # ]
    # COND.csv:
    columns = [
    	'PLT_CN',
    	'INVYR',
    	'CONDID',
    	'STATECD',
    	'COUNTYCD',
    	'CYCLE',
    	'PLOT',
    	'COND_STATUS_CD',
    	'FORTYPCD',
    	'STDAGE',
    	'SLOPE',
    	'ASPECT',
    	'OWNCD',
    	'OWNGRPCD',
    	'FIRE_SRS',
    	'DSTRBCD1',
    	'DSTRBYR1',
    	'DSTRBCD2',
    	'DSTRBYR2',
    	'DSTRBCD3',
    	'DSTRBYR3',
    ]

    dtype_dict = {
    	'PLT_CN': ,
    	'INVYR',
    	'CONDID',
    	'STATECD',
    	'COUNTYCD',
    	'CYCLE',
    	'PLOT',
    	'COND_STATUS_CD',
    	'FORTYPCD',
    	'STDAGE',
    	'SLOPE',
    	'ASPECT',
    	'OWNCD',
    	'OWNGRPCD',
    	'FIRE_SRS',
    	'DSTRBCD1',
    	'DSTRBYR1',
    	'DSTRBCD2',
    	'DSTRBYR2',
    	'DSTRBCD3',
    	'DSTRBYR3',
    }

    # PLOTSNAP.csv: columns_to_include = ['CN', 'INVYR', 'STATECD', 'COUNTYCD', 'CYCLE', 'PLOT']

    chunky_monkey_csv_loader(csv_file_path=big_csv_file,
                             sqlalchemy_engine=sql_db.engine,
                             sql_schema_name='dbo',
                             sql_table_name='COND',
                             included_columns=columns,
                             chunk_size=100)
