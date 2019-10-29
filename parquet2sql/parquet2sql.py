#!/usr/bin/python3

import argparse
import logging

import numpy
import pandas
import sqlalchemy


def get_parser():
    parser = argparse.ArgumentParser(
        description="Util to write a Parquet file to an SQL database.")
    parser.add_argument("--parquet", required=True, help="Parquet file to write to SQL database.")
    parser.add_argument(
        "--db", default="postgresql://superset:superset@localhost:5432/superset",
        help="the URL that indicates database dialect and connection arguments. The string form "
             "of the URL is ``dialect[+driver]://user:password@host/dbname[?key=value..]``, "
             "where ``dialect`` is a database name such as ``mysql``, ``oracle``, ``postgresql``, "
             "etc., and ``driver`` the name of a DBAPI, such as ``psycopg2``, ``pyodbc``, "
             "``cx_oracle``, etc.")
    parser.add_argument("--table", required=True, help="Name of the table in the database.")
    return parser


class ParquetManager:
    """Make incremental updates to an sql table using data extracted from a parquet file."""

    log = logging.getLogger("parquet2sql")
    UPDATE_COLUMN = "op"

    def __init__(self, db: str):
        """
        Initialize a `ParquetManager`.

        :param db: URL that indicates database dialect and connection arguments. The string form
                   of the URL is ``dialect[+driver]://user:password@host/dbname[?key=value..]``
                   where ``dialect`` is a database name such as ``mysql``, ``oracle``,
                  ``postgresql``, etc., and ``driver`` the name of a DBAPI, such as ``psycopg2``,
                  ``pyodbc``, ``cx_oracle``, etc.
        """
        self.db = db
        self.engine = sqlalchemy.create_engine(db)

    def process_parquet(self, parquet: str,  table: str):
        """
        Make an incremental update on the target sql table based on the value that the parquet \
        file contains on the column `op`.

        If a given row contains `op` = True the row will be appended to the table, and if `op`
        is false the row will be deleted from the table.

        :param parquet: Name of the parquet file that will be used to update the database.
        :param table: Name of the table that will be updated with the content of the parquet file.
        :return: None
        """
        df = self._load_parquet(parquet)
        self._incremental_delete(df, table)
        self._incremental_update(df, table)
        self.log.info("File was %s saved to table %s at %s", parquet, table, self.db)

    @staticmethod
    def _load_parquet(parquet: str):
        """Load the contents of a parquet file."""
        df = pandas.read_parquet(parquet)
        return df

    def _incremental_delete(self, df: pandas.DataFrame, table_name: str):
        """
        Delete rows on the target table that contain the same values as rows marked with \
        `op` = False in the loaded parquet file.

        :param df: DataFrame containing the data loaded from the parquet file.
        :param table_name: Name of the table to be updated.
        :return: None.
        """
        def filter_data(row, table):
            conds = [table.c[col] == row[col] for col in table.columns.keys()]
            return sqlalchemy.and_(*conds)
        if self.UPDATE_COLUMN not in df.columns:
            raise KeyError("There must be an `%s` column in the Parquet table."
                           % self.UPDATE_COLUMN)
        # Map the Inventory table in your database to a SQLAlchemy object
        meta = sqlalchemy.MetaData()
        table = sqlalchemy.Table(table_name, meta, autoload=True, autoload_with=self.engine)
        delete_df = df.loc[numpy.logical_not(df[self.UPDATE_COLUMN])]
        delete_condtion = delete_df.apply(lambda x: filter_data(x, table), axis=1).values.tolist()
        delete_condtion = sqlalchemy.or_(*delete_condtion)
        # Execute delete
        delete = table.delete().where(delete_condtion)
        with self.engine.connect() as conn:
            conn.execute(delete)

    def _incremental_update(self, df, table):
        """
        Add to the target sql table the content of the rows marked with `op` = True in the \
        parquet file.

        :param df: DataFrame containing the data loaded from the parquet file.
        :param table: Name of the table to be updated.
        :return: None
        """
        if self.UPDATE_COLUMN not in df.columns:
            raise KeyError("There must be an `%s` column in the Parquet table."
                           % self.UPDATE_COLUMN)
        add_data = df.loc[df[self.UPDATE_COLUMN]].drop(self.UPDATE_COLUMN, axis=1)
        add_data.to_sql(table, self.engine, if_exists="append", index=False)


def parquet2sql(parquet: str, table: str, db: str):
    """
    Make an incremental update on an sql table based on the contents of a parquet file.

    The loaded parquet file requires a column named `op` that contains a boolean value. If `op` \
    is True, the corresponding row will be added to the target sql table. It `op` is False, the \
    row will be deleted from the target table.

    :param parquet: Path to the parquet file to write to SQL database.
    :param table: Name of the table in the database that will be updated with the parquet file.
    :param db: URL that indicates database dialect and connection arguments. The string form
                   of the URL is ``dialect[+driver]://user:password@host/dbname[?key=value..]``
                   where ``dialect`` is a database name such as ``mysql``, ``oracle``,
                  ``postgresql``, etc., and ``driver`` the name of a DBAPI, such as ``psycopg2``,
                  ``pyodbc``, ``cx_oracle``, etc.
    :return: None
    """
    manager = ParquetManager(db=db)
    manager.process_parquet(parquet=parquet, table=table)
