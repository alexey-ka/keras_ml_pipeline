from operators.MLOperator import MLOperator
import pandas as pd
import config


class Evaluation(MLOperator):
    def __init__(self, database, params, ml_job_id, test_features, test_labels):
        """
        Models evaluation/comparison class
        :param database: database connection DBConnection
        :param params: {    'verbose': 0, # verbose level
                                    'metrics': # 'MAE' name of the used metrics
                                }
        :param ml_pipeline_id: id of the ML pipeline
        :param test_features: test features from the preprocessing operator
        :param test_labels: test labels from the preprocessing operator
        """
        self.database = database
        self.meta_info['value'] = 'evaluation'
        self.ml_job_id = ml_job_id
        self.model_results = {}
        self.test_features = test_features
        self.test_labels = test_labels
        self.params = params

    def add_model_to_evaluate(self, model):
        """
        Add model to the evaluation set
        :param model: MLModel
        :return:
        """
        self.model_results[model.operator_id] = model.evaluate(self.test_features, self.test_labels)

    def evaluate(self):
        """
        Get a pandas dataframe with the values
        :return:
        """
        return pd.DataFrame.from_dict(self.model_results, orient='index').reset_index().rename(
            columns={'index': 'operator_id', 0: 'metrics_value'})

    def log_meta_info(self):
        """
        Log the operator's meta data
        :return:
        """
        meta_info_df = self.evaluate()
        meta_info_df['ml_job_id'] = self.ml_job_id
        meta_info_df['metrics_id'] = 1
        self.database.insert_batch(meta_info_df, config.meta_data_eval_table)
