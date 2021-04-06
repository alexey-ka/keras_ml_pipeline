import tensorflow as tf

from datetime import datetime, timezone
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.layers.experimental import preprocessing

import config


class MLOperator:
    """
    General class of an ML Operator

    When initialisied always has meta_info data
    """

    meta_info = {'dict_table_name': 'mlmeta.ml_operators',
                 'id_col_name': 'operator_id',
                 'col_name': 'operator_name'
                 }

    def __init__(self, database, ml_job_id):
        """

        :param database - database connection DBConnection
        :param ml_pipeline_id - id of the whole pipeline:
        """
        self.database = database
        self.ml_job_id = ml_job_id

    def get_operator_id(self):
        """
        Find an id of the ML operator by its name
        :return: id (int)
        """
        return self.database.find_dict_id(**self.meta_info)

    def start_operator(self):
        """
        Log start event of the operator's execution
        """
        # list of the affected columns and its values
        self.operator_id = self.get_operator_id()
        cols = ['ml_job_id', 'operator_id', 'start_time', 'finished']
        vals = [self.ml_job_id, self.get_operator_id(), datetime.now(timezone.utc), False]
        self.database.insert_with_id(config.ml_operators_jobs_table, cols, vals, config.ml_job_id_column)

    def finalize_operator(self):
        """
        Finalise DB entry about the operator
        """
        where_clause = '{} = {} AND {} = {}'.format(config.ml_job_id_column, self.ml_job_id, config.operator_id_column,
                                                    self.get_operator_id())
        self.database.set_end_time(config.ml_operators_jobs_table, where_clause)
        return True
