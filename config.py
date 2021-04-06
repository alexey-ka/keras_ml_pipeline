db_params_dic = {
    "host"      : "localhost",
    "database"  : "autompg_ml",
    "user"      : "postgres",
    "password"  : "mypass"
}

meta_data_preprocessing_table = 'mlmeta.preprocessing_meta_data'
meta_data_train_table = 'mlmeta.train_meta_data'
meta_data_eval_table = 'mlmeta.evaluation_meta_data'

ml_operators_jobs_table = 'mlmeta.ml_operators_jobs'
ml_pipeline_jobs_table = 'mlmeta.ml_jobs'

ml_job_id_column = 'ml_job_id'
operator_id_column = 'operator_id'
max_conn_attempts = 5
connection_waiting_time = 5