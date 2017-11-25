#!/usr/bin/env bash

python nobench_wrapper.py \
   --db_url localhost:1521/test \
   --user test \
   --passwd test \
   --workers 2 \
   --threads 2 \
   --query 1 \
   --table NOBENCH \
   --recordcount 5 \
   --range 0 1000 \
   --interval 10 #\
#   --str1 data/1million_stats_str1.dat \
#   --dyn1 data/1million_stats_dyn1.dat \
#   --nested data/1million_stats_nested.dat \
#   --sparse data/1million_stats_nested.dat
