import numpy as np
import pandas as pd

import config
from operators.MLOperator import MLOperator
from tensorflow.keras.layers.experimental import preprocessing


class Preprocessing(MLOperator):
    """
    Operator to execute data preprocessing under the given parameters
    """
    def __init__(self, database, ml_job_id, params):
        """
        Operator to execute data preprocessing under the given parameters
        :param database: database connection DBConnection
        :param ml_job_id: id of the ML pipeline
        :param params: parameters of the operator:
                {
                table_name: schema.table_name of the table with data
                columns: [] list of the required columns
                'preprocessing_rules': {'col_name': column name, rule:{}} replace values
                'split_params': {'frac': 0.8, 'random_state': 0} train/test split params
                'target': target column (str)
        """
        self.params = params
        self.database = database
        self.dataset = None
        self.meta_info['value'] = 'preprocessing'
        self.ml_job_id = ml_job_id

    def execute_full_pipeline(self):
        """
        Execute the operator
        :return:
        """
        self.get_datasset()
        self.preprocess(self.params['preprocessing_rules'])
        self.split(self.params['split_params'])
        self.normalize()
        return self.train_features, self.test_features, self.train_labels, self.test_labels, self.normalizer

    def log_meta_info(self):
        """
        Log the operator's meta data
        :return:
        """
        meta_info_df = self.dataset.describe().transpose().add_prefix('df_').reset_index().rename(columns={'index':'col_name'})
        meta_info_df['ml_job_id'] = self.ml_job_id
        meta_info_df['operator_id'] = self.operator_id
        meta_info_df.columns = [x if '%' not in x else x.replace('%','') for x in meta_info_df.columns ]
        self.database.insert_batch(meta_info_df, config.meta_data_preprocessing_table)

    def get_datasset(self):
        """
        Retrieve the dataset from DB
        :return:
        """
        self.dataset = self.get_df_from_db(self.database, self.params['table_name'], self.params['columns'])

    def get_df_from_db(self, db_connection, df_table_name, columns):
        """
        Connection to retrieve the dataset
        :param db_connection:
        :param df_table_name:
        :param columns:
        :return:
        """
        sql_query = "SELECT {} FROM {}".format(','.join(columns), df_table_name)
        dataset = db_connection.get_dataframe(sql_query, columns)
        return dataset

    def preprocess(self, replace_values, df=None):
        """
        Dataset preprocessing
        :param replace_values: {'col_name': column name, rule:{}} replace values
        :return:
        """
        if df is not None:
            dataset = df.dropna()
        else:
            dataset = self.dataset.dropna()
        dataset[replace_values['col_name']] = dataset[replace_values['col_name']].map(replace_values['rule'])
        dataset = pd.get_dummies(dataset, columns=[replace_values['col_name']],
                                 prefix=replace_values['prefix'], prefix_sep=replace_values['prefix_sep'])
        self.dataset = dataset
        return dataset

    def split(self, params):
        """
        Split into train/test subset
        :param params: {'frac': 0.8, 'random_state': 0} train/test split params
        :return:
        """
        self.train_features = self.dataset.sample(frac=params['frac'], random_state=params['random_state'])
        self.test_features = self.dataset.drop(self.train_features.index)

        self.train_labels = self.train_features.pop(self.params['target'])
        self.test_labels = self.test_features.pop(self.params['target'])

    def normalize(self):
        """
        Normalize the dataset
        :return:
        """
        self.normalizer = preprocessing.Normalization()
        self.normalizer.adapt(np.array(self.train_features))
