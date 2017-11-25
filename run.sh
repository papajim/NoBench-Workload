#!/usr/bin/env bash

python nobench_wrapper.py \
   --db_url localhost:1521/test \
   --user test \
   --passwd test \
   --workers 2 \
   --threads 2 \
   --query 1 \
   --table NOBENCH \
   --recordcount 100 \
   --range 0 1000 \
   --interval 10 \
   --str1 str1.in \
   --nested nested.in \
   --sparse sparse.in

