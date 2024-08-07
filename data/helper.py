"""helper functions"""
import pandas as pd
import pyodbc
import logging

from sqlalchemy import create_engine, text


DATABASE = "d4hdbd44mip8rs"
USER = "uad5b4pgen90bu"
PASSWORD = "p1e5bab8291c21f7f9f0ecb76e417c9d397cd5f069d6b03285044b807649ce986"
HOST = "c98cgvoa4cjnvf.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com"

connection_string = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}/{DATABASE}"
engine = create_engine(connection_string)

logging.basicConfig(
    filename="gcis.log", level=logging.INFO, format="%(asctime)-15s %(message)s"
)
logger = logging.getLogger()


def is_table_existing(tbl_name):
    query = f"select exists(select * from information_schema.tables where table_name='{tbl_name}')"

    with engine.connect() as conn:
        result = conn.execute(text(query))
        return result.first()[0]


def delete_table(tbl_name):
    query = f"delete from {tbl_name}; ALTER SEQUENCE {tbl_name}_id_seq RESTART;"
    tbl_exists = is_table_existing(tbl_name)

    if tbl_exists:
        with engine.connect() as conn:
            conn.execute(text(query))
    return


def insert_data_into_db(query, restart=False, tbl_name=None):
    """
    Insert data into database.
    """
    with engine.connect() as conn:
        if not tbl_name:
            raise Exception(f"Table name is required.")
        if restart:
            delete_table(tbl_name)
        conn.execute(text(query))
    return


def run_query_from_db(query):
    """run query from database"""
    with engine.connect() as conn:
        conn.execute(text(query))
    return


def get_data_from_db(query):
    """Pull data from database."""
    conn = None
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


def get_data_from_cams(query):
    """connect to CAMS database with windows authorization
    and run query given
    """
    from sqlalchemy.engine import URL
    print('>>>>>> CAMS <<<<<<<<')

    connection_string = "DRIVER={SQL Server};SERVER=gc-sql-aws;DATABASE=CAMS_Enterprise;Trusted_Connection=yes;"
    connection_url = URL.create(
        "mssql+pyodbc", query={"odbc_connect": connection_string}
    )
    engine = create_engine(connection_url)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


def df_to_sql(df, col_names, col_types, tbl_name, tbl_cols):
    """
    Generate sql code to run from local data.
    """

    def get_col(col_value, col_type):
        if col_type == "num":
            return str(col_value)
        else:
            return f"'{col_value}'"

    for col in col_names:
        if df[col].dtype == "object":
            df[col] = df[col].str.replace("'", "''")

    tbl_cols = "(" + ", ".join(tbl_cols) + ")"
    query = [f"insert into {tbl_name} {tbl_cols} values"]
    num_row, num_col = df.shape

    for i, row in df.iterrows():
        row_query = []
        for col_name, col_type in zip(col_names, col_types):
            col_value = row[col_name]
            row_query.append(get_col(col_value, col_type))
        row_query = ", ".join(row_query)
        if i == num_row - 1:
            row_query = "(" + row_query + ");"
        else:
            row_query = "(" + row_query + "),"
        query.append(row_query)

    return " ".join(query)


def datetime_to_time(dt):
    try:
        return dt.time()
    except:
        return None
