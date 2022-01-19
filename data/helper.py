"""helper functions"""
import psycopg2
import pandas as pd
import pyodbc



DATABASE = 'decst491stvjr1'
USER = 'eljdsejcytlyyy'
PASSWORD = '3aa6f140063d6c38b94d104d44b44d226d886ac81bb51cb31ae0dfcc4704a82d'
HOST = 'ec2-52-44-55-63.compute-1.amazonaws.com'


def delete_table(tbl_name):
    query = f"delete from {tbl_name}; ALTER SEQUENCE {tbl_name}_id_seq RESTART;"
    conn = None
    flag = is_table_existing(tbl_name)
    try:
        conn = psycopg2.connect(database=DATABASE,
                                user=USER,
                                password=PASSWORD,
                                host=HOST)
        if flag:
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            cur.close()
            conn.close()
    return


def is_table_existing(tbl_name):
    query = f"select exists(select * from information_schema.tables where table_name='{tbl_name}')"
    conn = None
    flag = False
    try:
        conn = psycopg2.connect(database=DATABASE,
                                user=USER,
                                password=PASSWORD,
                                host=HOST)
        cur = conn.cursor()
        cur.execute(query)
        flag = cur.fetchone()[0]
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            cur.close()
            conn.close()
    return flag


def insert_data_into_db(query, restart=False, tbl_name=None):
    """
    Insert data into database.
    """
    conn = None
    try:
        conn = psycopg2.connect(database=DATABASE,
                                user=USER,
                                password=PASSWORD,
                                host=HOST)
        cur = conn.cursor()
        if restart and tbl_name:
            cur.execute(
                f"delete from {tbl_name}; ALTER SEQUENCE {tbl_name}_id_seq RESTART;")
            conn.commit()
        cur.execute(query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            cur.close()
            conn.close()
    return


def delete_data_from_db(query):
    """delete rows from database"""
    conn = None
    try:
        conn = psycopg2.connect(database=DATABASE,
                                user=USER,
                                password=PASSWORD,
                                host=HOST)
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return


def get_data_from_db(query):
    """Pull data from database."""
    conn = None
    try:
        conn = psycopg2.connect(database=DATABASE,
                                user=USER,
                                password=PASSWORD,
                                host=HOST)
        df = pd.read_sql(query, conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return df


def get_data_from_cams(query):
    """connect to CAMS database with windows authorization
    and run query given
    """
    conn = pyodbc.connect(
        r'DRIVER={SQL Server};'
        r'SERVER=gc-sql-aws;'
        r'DATABASE=CAMS_Enterprise;'
        r'Trusted_Connection=yes;'
    )
    df = pd.read_sql(query, conn)
    return df


def df_to_sql(df, col_names, col_types, tbl_name, tbl_cols):
    """
    Generate sql code to run from local data.
    """

    def get_col(col_name, col_type):
        if col_type == 'num':
            return str(col_name)
        else:
            return f"'{col_name}'"

    tbl_cols = '(' + ', '.join(tbl_cols) + ')'
    query = [f'insert into {tbl_name} {tbl_cols} values']
    num_row, num_col = df.shape

    for i, row in df.iterrows():
        row_query = []
        for col_name, col_type in zip(col_names, col_types):
            row_query.append(get_col(row[col_name], col_type))
        row_query = ', '.join(row_query)
        if i == num_row-1:
            row_query = '(' + row_query + ');'
        else:
            row_query = '(' + row_query + '),'
        query.append(row_query)

    return ' '.join(query)


def datetime_to_time(dt):
    try:
        return dt.time()
    except:
        return None
