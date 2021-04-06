from datetime import datetime, timezone
import config

from operators.Evaluator import Evaluation
from operators.MLmodel import MLModel
from operators.preprocess import Preprocessing


class MLPipeline:
    def __init__(self, database):
        """
        Create an ML pipeline
        :param database: database connection DBConnection
        :param stages: order of the stages  [ {'stage_name':'name',
                                                'stage_operator':operator,
                                                'params':[]
                                                'confirm_success_of_previous_operator':True/False
                                                }...]
        """
        self.pipeline = []
        self.database = database
        self.db_table_name = config.ml_pipeline_jobs_table
        self.ml_job_id = None
        self.start_pipeline()

    def execute_pipeline(self):
        models = []
        for stage in self.pipeline:
            if stage['name'] == 'preprocess':
                preprocessing = Preprocessing(self.database, self.ml_job_id, stage['params'])
                preprocessing.start_operator()
                self.train_features, self.test_features, self.train_labels, self.test_labels, self.normalizer = preprocessing.execute_full_pipeline()
                preprocessing.finalize_operator()
            elif stage['name'] == 'train':
                model = MLModel(self.database, stage['params'], self.ml_job_id, self.normalizer,
                                self.train_features, self.train_labels)
                model.start_operator()
                model.init_model(self.normalizer)
                model.fit_model(self.train_features, self.train_labels)
                model.log_meta_info()
                model.finalize_operator()
                models.append(model)

            elif stage['name'] == 'evaluate':
                evaluation = Evaluation(self.database, stage['params'], self.ml_job_id, self.test_features,
                                        self.test_labels)
                evaluation.start_operator()
                for model in models:
                    evaluation.add_model_to_evaluate(model)
                evaluation.log_meta_info()
                evaluation.finalize_operator()

    def start_pipeline(self):
        cols = ['start_time', 'finished']
        vals = [datetime.now(timezone.utc), False]
        self.ml_job_id = self.database.insert_with_id(self.db_table_name, cols, vals, config.ml_job_id_column)

    def finalize_pipeline(self):
        if self.ml_job_id is not None:
            where_clause = '{} = {}'.format(config.ml_job_id_column, self.ml_job_id)
            self.database.set_end_time(self.db_table_name, where_clause)
        self.database.close_connection()

    def add_operator(self, operator):
        self.pipeline.append(operator)
