#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER "$DB_USER";
    CREATE DATABASE "$DB_NAME";
    GRANT ALL PRIVILEGES ON DATABASE "$DB_NAME" TO "$DB_USER";

EOSQL


psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$DB_NAME" <<-EOSQL
CREATE SCHEMA IF NOT EXISTS $DATA_SCHEMA_NAME;
CREATE SCHEMA IF NOT EXISTS $ML_SCHEMA_NAME;

CREATE TABLE IF NOT EXISTS "$DATA_SCHEMA_NAME"."auto-mpg-ds" (
  MPG real,
  Cylinders real,
  Displacement real,
  Horsepower real,
  Weight real,
  Acceleration real,
  Model_Year integer,
  Origin integer,
  model_name VARCHAR(50)
);
COPY "data"."auto-mpg-ds" FROM '/media/data/auto-mpg.data' DELIMITER ' ' QUOTE '"' CSV NULL AS '?';

CREATE TABLE IF NOT EXISTS "$DATA_SCHEMA_NAME"."auto-mpg-ds" (
  MPG real,
  Cylinders real,
  Displacement real,
  Horsepower real,
  Weight real,
  Acceleration real,
  Model_Year integer,
  Origin integer,
  model_name VARCHAR(50)
);


CREATE TABLE IF NOT EXISTS "$ML_SCHEMA_NAME"."ml_operators" (
  operator_id SERIAL,
  operator_name VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS "$ML_SCHEMA_NAME"."ml_operators_jobs" (
  ml_job_id integer,
  operator_id integer,
  start_time timestamp,
  end_time timestamp,
  finished bool
);

CREATE TABLE IF NOT EXISTS "$ML_SCHEMA_NAME"."ml_jobs" (
  ml_job_id SERIAL,
  start_time timestamp,
  end_time timestamp,
  finished bool
);

CREATE TABLE IF NOT EXISTS "$ML_SCHEMA_NAME"."evaluation_meta_data" (
  metrics_id integer,
  operator_id integer,
  ml_job_id integer,
  metrics_value real
);

CREATE TABLE IF NOT EXISTS "$ML_SCHEMA_NAME"."evaluation_metrics" (
  metrics_id SERIAL,
  property_name VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS "$ML_SCHEMA_NAME"."train_meta_data" (
  ml_job_id integer,
  operator_id integer,
  loss real,
  val_loss real,
  epoch integer
);

CREATE TABLE IF NOT EXISTS "$ML_SCHEMA_NAME"."preprocessing_meta_data" (
  ml_job_id integer,
  operator_id integer,
  col_name VARCHAR(30),
  df_count real,
  df_mean real,
  df_min real,
  df_std real,
  df_25 real,
  df_50 real,
  df_75 real,
  df_max real
);



INSERT INTO "$ML_SCHEMA_NAME"."evaluation_metrics" (property_name) VALUES ('MAE');
INSERT INTO "$ML_SCHEMA_NAME"."ml_operators" (operator_name) VALUES ('preprocessing');
INSERT INTO "$ML_SCHEMA_NAME"."ml_operators" (operator_name) VALUES ('linear_model');
INSERT INTO "$ML_SCHEMA_NAME"."ml_operators" (operator_name) VALUES ('dnn_model');
INSERT INTO "$ML_SCHEMA_NAME"."ml_operators" (operator_name) VALUES ('evaluation');
EOSQL