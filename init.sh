#!/bin/bash
sed 's/ \{1,\}/ /g' ./db_init/auto-mpg.data>./db_init/auto-mpg2.data
sed $'s/ *\t/ /g' ./db_init/auto-mpg2.data > ./db_init/auto-mpg.data
rm ./db_init/auto-mpg2.data
docker build -t ml_pipeline_a .
docker-compose up -d
