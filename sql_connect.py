import os

import pandas as pd
import pyodbc


# Example:
# >>> df = laod_data_pd('MyTable', '*')
# --- do some stuff to df ---
# >>> schema = find_schema(df)
# >>> create_table('MyTableUpdated', df, schema)


def load_df(table_name):
    """
    Create a pandas dataframe from the given table. Can also use load_data_pd
    (below) -- it's faster.
    """
    data = load_data(table_name, '*')
    column_names = load_cols(table_name)
    df = pd.DataFrame([row[1:] for row in data], columns=column_names)
    return df

def load_cols(table_name):
    """
    Obtain the column names to use in the pandas dataframe.
    """
    connection = sql_connection()
    query = """SELECT * FROM {}.INFORMATION_SCHEMA.COLUMNS
               WHERE TABLE_NAME = N'{}'""".format(
               os.environ['sql_database'], table_name)
    with connection.cursor() as cur:
        cur.execute(query)
        info_schema = cur.fetchall()
    # COLUMN_NAME is the third field in INFORMATION_SCHEMA
    column_names = [row[3] for row in info_schema]
    connection.commit()
    connection.close()
    return column_names

def load_data(table_name, id_column, columns):
    """
    Load specified columns from the given table, return the entire
    dataset as a list of lists.
    """
    connection = sql_connection()
    if type(columns) == list:
        columns = ", ".join(columns)
    load_query = ("""SELECT {}, {} FROM [dbo].[{}]""".format(
        id_column,
        columns,
        table_name))
    print(load_query)
    with connection.cursor() as cur:
        cur.execute(load_query)
        data = cur.fetchall()
    data = [list(row) for row in data]
    connection.commit()
    connection.close()
    return data

def load_data_pd(table_name, columns):
    """
    Load the specified columns into a pandas dataframe.
    """
    connection = sql_connection()
    if type(columns) == list:
        columns = ", ".join(columns)
    load_query = ("""SELECT {} FROM [dbo].[{}]""".format(
        columns,
        table_name))
    print(load_query)
    df = pd.read_sql_query(load_query, connection)
    connection.commit()
    connection.close()
    return df

def create_table(table_name, data, schema):
    """
    Create (replace if needed) a table in the environment connection with the
    given table name.

    table_name: target table to create
    data: array of values (not a dataframe)
    schema: a list of (column, type) pairs. It's assumed that the first pair is
            the primary key.
    """
    connection = sql_connection()
    create_query = ("""CREATE TABLE [dbo].[{}] ({} {} primary key, """.format(
                        table_name,
                        schema[0][0],
                        schema[0][1]))
    create_query += ', '.join([item[0] + ' ' + item[1] for item in schema[1:]])
    create_query += ")"
    print(create_query)
    with connection.cursor() as cur:
        if cur.tables(table=table_name, tableType='TABLE').fetchone():
            try:
                cur.execute("DROP TABLE [dbo].[{}]".format(table_name))
                print("dropped table {}".format(table_name))
            except pyodbc.Error as e:
                print("ERROR: {}".format(e))
                connection.rollback()

    with connection.cursor() as cur:
        try:
            cur.execute(create_query)
            print("created table {}".format(table_name))
        except pyodbc.Error as e:
            print("ERROR: {}".format(e))
            connection.rollback()

    copy_query = ("insert into [dbo].[{}] (".format(table_name))
    copy_query += ', '.join(["[{}]".format(item[0]) for item in schema])
    copy_query += ") values ("
    copy_query += ', '.join("?" for item in schema)
    copy_query += ")"
    print(copy_query)

    for row in data:
        row = ((copy_query), *row)
        with connection.cursor() as cur:
            try:
                cur.execute(*row)
            except pyodbc.Error as e:
                print("ERROR: {}".format(e))
                connection.rollback()
                break
    connection.commit()
    connection.close()

def find_schema(df):
    """
    Obtain the column names & types from a pandas df. Convert to MSSQL types.
    """
    DATATYPE_MAPPING = {
        "int64": "int",
        "float64": "float",
        "object": "varchar(20)",
        "bool": "bit",
        }
    types = [str(type) for type in df.dtypes]
    schema = [(
        df.columns[i],
        DATATYPE_MAPPING[type]) for i, type in enumerate(types)]
    return schema

def sql_connection():
    """
    Connect to the SQL server via pyodbc
    """
    connection = pyodbc.connect(
        driver=os.environ['sql_driver'],
        server=os.environ['sql_server'],
        database=os.environ['sql_database'],
        uid=os.environ['sql_user'],
        pwd=os.environ['sql_pwd'],
    )
    return connection
