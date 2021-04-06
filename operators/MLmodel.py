import tensorflow as tf
import pandas as pd
from operators.MLOperator import MLOperator

from tensorflow import keras
from tensorflow.keras import layers
import config

class MLModel(MLOperator):
    """
    An ML operator to train a Tensorflow model according to the given parameters
    """
    def __init__(self, database, params, ml_job_id, normalizer, train_features, train_labels):
        """
        An ML operator to train a Tensorflow model according to the given parameters
        :param database: database connection DBConnection
        :param params: parameters of the operator: {
                'model_hyperparams': {
                    'epochs': 100,  # n of the epochs
                    'verbose': 0, # verbost level
                    'validation_split': 0.2, # validation split
                    'learning_rate': 0.1, # learning rate
                    'layers': [{'n': 64, 'activation': 'relu'}, {'n': 64, 'activation': 'relu'}], # description of the NN layers. [] if linear regression.
                    'loss': 'mean_absolute_error' # loss function
                },
                'execute': True, # automatically execure the operator
                'model_name': 'dnn_model' # name of the model

        }
        :param ml_pipeline_id: id of the ML pipeline
        :param normalizer: data normalizer from the preprocessing operator
        :param train_features: train features from the preprocessing operator
        :param train_labels: train labels from the preprocessing operator
        """
        self.database = database
        self.meta_info['value'] = params['model_name']
        self.ml_job_id = ml_job_id
        self.normalizer = normalizer
        self.train_features = train_features
        self.train_labels = train_labels
        self.model_hyperparams = params['model_hyperparams']

    def init_model(self, normalizer):
        """
        Init the model with a normalizer
        :param normalizer:
        :return:
        """
        model_layers = [normalizer]
        # add the hidden NN layers
        for i in range(len(self.model_hyperparams['layers'])):
            model_layers.append(layers.Dense(self.model_hyperparams['layers'][i]['n'],
                                             activation=self.model_hyperparams['layers'][i]['activation']))
        model_layers.append(layers.Dense(1))
        model = keras.Sequential(model_layers)
        model.compile(
            optimizer=tf.optimizers.Adam(learning_rate=self.model_hyperparams['learning_rate']),
            loss=self.model_hyperparams['loss'])
        self.model = model

    def fit_model(self, train_features, train_labels):
        """
        Fit the ML model
        :param train_features:
        :param train_labels:
        :return:
        """
        history = self.model.fit(
            train_features, train_labels,
            epochs=self.model_hyperparams['epochs'],
            # suppress logging
            verbose=self.model_hyperparams['verbose'],
            # Calculate validation results on 20% of the training data
            validation_split=self.model_hyperparams['validation_split'])
        self.history = history

    def log_meta_info(self):
        """
        Log the operator's meta data
        """
        meta_info = self.history.history
        meta_info['epoch'] = self.history.epoch
        meta_info_df = pd.DataFrame(meta_info)
        meta_info_df['ml_job_id'] = self.ml_job_id
        meta_info_df['operator_id'] = self.operator_id
        self.database.insert_batch(meta_info_df, config.meta_data_train_table)

    def evaluate(self, test_features, test_labels):
        """
        Model evaluation
        :param test_features:
        :param test_labels:
        :return:
        """
        return self.model.evaluate(
            test_features, test_labels, verbose=self.model_hyperparams['verbose'])
