import random
import time
import cx_Oracle
import threading
from nobench_queries import QueryHandler

class NoBenchWorker:
    def __init__(self, pipe, lock, db_params, queryType, threads, recordcount, lookup_files, batchSize=None):
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
        self.lookup_files = lookup_files
        self.conn_pool = None

    def runThread(self):
        #conn = self.conn_pool.acquire()
        #cur = con.cursor()
        handler = QueryHandler(self.queryType, self.table, self.range, self.lookup_files)
        for i in xrange(self.recordcount):
            handler.run()
        #cur.close()
        #conn.release()

    def runThreadTest(self):
        out_p, in_p = self.pipe
        out_p.close()
        handler = QueryHandler(self.queryType, self.table, self.range, self.lookup_files)
        time.sleep(10)
        for i in xrange(self.recordcount):
            handler.run()
            self.lock.acquire()
            in_p.send([random.randrange(1, 100), random.randrange(1,100), random.randrange(1,100)])
            self.lock.release()
        time.sleep(10)

    def start(self):
        #self.conn_pool = cx_Oracle.SessionPool(self.user, self.passwd, self.db_url, min = 2, max = 10, increment = 1, threaded = True)
        for i in xrange(self.threads-1):
            thread = threading.Thread(name="#"+str(i), target=self.runThreadTest)
            self.threadArray.append(thread)
            thread.start()
        
        self.runThreadTest()

        for thread in self.threadArray:
            thread.join()
