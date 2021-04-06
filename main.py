import time
import config
from MLPipeline import MLPipeline
from db_utils import DBConnection

preprocessing_params = {
    'table_name': '"data"."auto-mpg-ds"',
    'columns': ['MPG', 'Cylinders', 'Displacement', 'Horsepower', 'Weight',
                'Acceleration', 'model_year', 'Origin'],
    'preprocessing_rules':
        {'col_name': 'Origin', 'rule': {1: 'USA', 2: 'Europe', 3: 'Japan'}, 'prefix': '', 'prefix_sep': ''},
    'split_params': {'frac': 0.8, 'random_state': 0},
    'target': 'MPG',
    'execute': True
}

linear_regression_params = {
    'model_hyperparams': {
        'epochs': 100,
        'verbose': 0,
        'validation_split': 0.2,
        'learning_rate': 0.1,
        'layers': [],
        'loss': 'mean_absolute_error'
    },
    'execute': True,
    'model_name': 'linear_model'
}

dnn_params = {
    'model_hyperparams': {
        'epochs': 100,
        'verbose': 0,
        'validation_split': 0.2,
        'learning_rate': 0.1,
        'layers': [{'n': 64, 'activation': 'relu'}, {'n': 64, 'activation': 'relu'}],
        'loss': 'mean_absolute_error'
    },
    'execute': True,
    'model_name': 'dnn_model'
}

eval_params = {
    'verbose': 0,
    'metrics': 'MAE'
}

database = DBConnection()
attempts = config.max_conn_attempts

while attempts > 0 and not database.has_connection:
    # try to reconnect
    time.sleep(config.connection_waiting_time)
    database.connect()
    attempts -= 1


pipeline = MLPipeline(database)

pipeline.add_operator({'name': 'preprocess', 'params': preprocessing_params})
pipeline.add_operator({'name': 'train', 'params': linear_regression_params})
pipeline.add_operator({'name': 'train', 'params': dnn_params})
pipeline.add_operator({'name': 'evaluate', 'params': eval_params})
pipeline.execute_pipeline()
pipeline.finalize_pipeline()
