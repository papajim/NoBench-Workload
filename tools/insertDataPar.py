#!/usr/bin/env python

import cx_Oracle
import threading

def dothework(rows, start, end):
    name = threading.currentThread().name
    con = cx_Oracle.connect("user", "pass", "localhost/orcl")
    con.autocommit = True
    cur = con.cursor()
    
    count = 0
    for i in xrange(start, end):
        cur.execute("INSERT INTO NOBENCH(NOBENCH_NUM, NOBENCH_DOC) values (:1, :2)", rows[i])
        count += 1
        if count % 1000 == 0:
            print "{0}: {1} -> {2}".format(name, i, end)

    con.commit()
    cur.close()
    con.close()

##########################################################

f = open("../data/1million_parsed.dat", "r")
flag = True
rows = []
while flag:
    line_in = f.readline()
    if line_in == "":
        break
    fields = line_in.replace("\n", "").split(",", 1)
    rows.append((int(fields[0]), fields[1]))
f.close()

threads = 10
interval = len(rows) / threads
threadArray = []

for i in xrange(threads):
    thread = threading.Thread(name="#"+str(i), target=dothework, args=(rows, i*interval, (i+1)*interval,))
    threadArray.append(thread)
    thread.start()

for thread in threadArray:
    thread.join()

