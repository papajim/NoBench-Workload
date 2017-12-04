#!/usr/bin/env python

import cx_Oracle

f = open("../data/1million_parsed.dat", "r")

con = cx_Oracle.connect("user", "pass", "localhost/orcl")
con.autocommit = False
cur = con.cursor()

count = 0
flag = True
while flag:
    rows = []
    for i in xrange(100):
        line_in = f.readline()
        if line_in == "":
            flag = False
            break
        fields = line_in.replace("\n", "").split(",", 1)
        rows.append((int(fields[0]), fields[1]))
    
    for i in xrange(len(rows)):
        cur.execute("INSERT INTO NOBENCH(NOBENCH_NUM, NOBENCH_DOC) values (:1, :2)", rows[i])
        if cur.rowcount != 1:
            print "ERROR IN INSERT"
            exit()
    
    con.commit()

    count += len(rows)
    print "Inserted "+str(count)

con.commit()
cur.close()
conn.close()
f.close()
