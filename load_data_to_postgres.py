import pandas as pd
from sqlalchemy import create_engine

DB_CONN_STRING = "postgresql://postgres:postgres@127.0.0.1:5432/ctran"

engine = create_engine(DB_CONN_STRING)

def load_data(df: pd.DataFrame, table_name: str):
  df.to_sql(table_name, engine, if_exists="append", index=False)