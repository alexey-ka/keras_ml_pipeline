"""
Functions to interact with PostgreSQL
"""
from datetime import datetime, timezone
import psycopg2
import config
import pandas as pd


class DBConnection():
    """
    Class to manage connection with PostgreSQL
    """

    def __init__(self):
        self.connection = self.connect()

    def connect(self):
        try:
            # connect to the PostgreSQL server
            conn = psycopg2.connect(**config.db_params_dic)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return None
        return conn

    @property
    def has_connection(self):
        """
        Check if db connection is established
        :return: True if connection existst, otherwise False
        """
        if self.connection is None:
            return False
        return True

    def get_dataframe(self, select_query, column_names):
        """
        Tranform a SELECT query into a pandas dataframe
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(select_query)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            cursor.close()
            return None
        tupples = cursor.fetchall()
        cursor.close()
        df = pd.DataFrame(tupples, columns=column_names)
        return df

    def insert_with_id(self, table_name, columns, values, return_column):
        """
        Create and execute an SQL statement to receive an automatically-incremented ID of the entry
        :param table_name: name of the SQL table including the schema id
        :param columns: list of the affected columns
        :param values: values of the affected collumns
        :param return_column: id of the column that should be retrieved from the DB
        :return:
        """
        sql_string = "INSERT INTO {} ({}) VALUES {} RETURNING {};".format(table_name, ','.join(columns),
                                                                           str(tuple(['%s'] * len(columns))).replace(
                                                                               "'", ""), return_column)
        cursor = self.connection.cursor()
        cursor.execute(sql_string, tuple(values))
        returned_id = cursor.fetchone()[0]
        self.connection.commit()
        cursor.close()
        return returned_id

    def set_end_time(self, table_name, where_clause):
        """
        Set end time to the executed pipeline or operator
        :param table_name:
        :param where_clause:
        """
        sql_string = "UPDATE {} SET end_time=%s, finished = TRUE WHERE {};".format(table_name, where_clause)
        cursor = self.connection.cursor()
        cursor.execute(sql_string, (datetime.now(timezone.utc),))
        self.connection.commit()
        cursor.close()

    def find_dict_id(self, dict_table_name, id_col_name, col_name, value):
        """
        Find an id of the required property by its name
        :param dict_table_name: name of the table where the value could be stored
        :param id_col_name: required column with the id
        :param col_name: column name where the searched value could be present
        :param value: required value
        :return: None or the found id
        """
        sql_string = "SELECT {} FROM {} WHERE {}=%s;".format(id_col_name, dict_table_name, col_name)
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql_string, (value,))
            returned_id = cursor.fetchone()[0]
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            cursor.close()
            return None
        cursor.close()
        return int(returned_id)

    def insert_batch(self, df, table):
        """
        Using cursor.executemany() to insert the dataframe
        """
        # Create a list of tupples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]
        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))
        # SQL quert to execute
        query = "INSERT INTO {}({}) VALUES{}".format(table, cols, str(tuple(['%s'] * len(tuples[0]))).replace("'", ""))
        cursor = self.connection.cursor()
        try:
            for t in tuples:
                cursor.execute(query, t)
                self.connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.connection.rollback()
            print(error)
            cursor.close()
            return error
        cursor.close()
        return True

    def close_connection(self):
        """
        Close connection to DB if exists
        """
        if self.has_connection:
            self.connection.close()
