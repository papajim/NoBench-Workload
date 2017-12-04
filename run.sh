#!/usr/bin/env bash

python nobench_wrapper.py \
   --db_url localhost:1521/test \
   --user test \
   --passwd test \
   --workers 2 \
   --threads 2 \
   --query 5 \
   --table NOBENCH \
   --recordcount 5 \
   --range 0 1000 \
   --interval 10 \
   --lookup data/1million_lookup.dat \
   --lookup_text data/1million_lookup_text.dat
