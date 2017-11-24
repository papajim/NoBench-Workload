import cx_Oracle
import threading
from nobench_queries import QueryHandler

class NoBenchWorker:
    def __init__(self, pipe, lock, db_params, queryType, threads, recordcount, batchSize=None):
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
        self.conn_pool = None

    def runThread(self):
        con = pool.acquire()
        cur = con.cursor()
        for i in xrange(self.recordcount):
            cur.execute("select * from dual")
            seqval, = cur.fetchone()
            print("Thread", threading.current_thread().name, "fetched sequence =", seqval)

    def runThreadTest(self):
        out_p, in_p = self.pipe
        out_p.close()
        for i in xrange(self.recordcount):
            self.lock.acquire()
            in_p.send(1)
            self.lock.release()

    def start(self):
        #self.conn_pool = cx_Oracle.SessionPool(self.user, self.passwd, self.db_url, min = 2, max = 10, increment = 1, threaded = True)
        for i in xrange(self.threads):
            thread = threading.Thread(name="#"+str(i), target=self.runThreadTest)
            self.threadArray.append(thread)
            thread.start()
        
        for thread in self.threadArray:
            thread.join()
