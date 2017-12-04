import random
import time
import cx_Oracle
import threading
from nobench_queries import QueryHandler

class NoBenchWorker:
    def __init__(self, pipe, lock, db_params, queryType, threads, recordcount, lookup_values, lookup_values_text, batchSize=None):
        self.db_url = db_params["db_url"]
        self.user = db_params["user"]
        self.passwd = db_params["passwd"]
        self.table = db_params["table"]
        self.range = db_params["range"]
        self.queryType = queryType
        self.threads = threads
        self.threadArray = []
        self.recordcount = recordcount
        self.batchSize = batchSize
        self.pipe = pipe
        self.lock = lock
        self.lookup_values = lookup_values
        self.lookup_values_text = lookup_values_text
        self.conn_pool = None

    def runThread(self):
        out_p, in_p = self.pipe
        out_p.close()
        conn = self.conn_pool.acquire()
        cur = conn.cursor()
        handler = QueryHandler(cur, self.queryType, self.table, self.range, self.lookup_values, self.lookup_values_text)
        for i in xrange(self.recordcount):
            s = time.time()
            handler.run()
            e = time.time()
            diff = e - s
            self.lock.acquire()
            in_p.send([diff, diff, diff])
            self.lock.release()
        cur.close()
        self.conn_pool.release(conn)

    def runThreadTest(self):
        out_p, in_p = self.pipe
        out_p.close()
        handler = QueryHandler(self.queryType, self.table, self.range, self.lookup_values, self.lookup_values_text)
        time.sleep(10)
        for i in xrange(self.recordcount):
            handler.run()
            self.lock.acquire()
            in_p.send([random.randrange(1, 100), random.randrange(1,100), random.randrange(1,100)])
            self.lock.release()
        time.sleep(10)

    def start(self):
        self.conn_pool = cx_Oracle.SessionPool(self.user, self.passwd, self.db_url, min = 2, max = 10, increment = 1, threaded = True)
        for i in xrange(self.threads-1):
            thread = threading.Thread(name="#"+str(i), target=self.runThread)
            self.threadArray.append(thread)
            thread.start()
        
        self.runThread()

        for thread in self.threadArray:
            thread.join()
