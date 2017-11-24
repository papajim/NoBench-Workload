import time
from argparse import ArgumentParser
from multiprocessing import Process, Pipe, Lock
from nobench_worker import NoBenchWorker

def runWorker(pipe, lock, db_params, queryType, threads, recordcount) :
    worker = NoBenchWorker(pipe, lock, db_params, queryType, threads, recordcount)
    worker.start()

################################################################################################

def monitor(pipe, interval):
    print "{0}\t{1}\t{2}\t{3}\t{4}".format("time", "ops/sec", "avgResponse", "minResponse", "maxResponse")
    out_p, in_p = pipe
    in_p.close()

    count = 0
    total = 0
    sumResponse = 0.0
    minResponse = 100000.0
    maxResponse = -1.0
    baseTime = time.time()*1000
    loopTime = time.time()
    while True:
        try:
            r = out_p.recv()
            total += 1
            count += 1
            sumResponse += r[0]
            if minResponse > r[1]: minResponse = r[1]
            if maxResponse < r[2]: maxResponse = r[2]

            nowTime = time.time()
            diff = nowTime - loopTime
            if (diff >= interval):
                avgResponse = sumResponse / (count * 1.0) if count > 0 else sumResponse
                print "{0:.0f}\t{1:.3f}\t{2:.3f}\t{3:.3f}\t{4:.3f}".format(round(nowTime*1000-baseTime), (count*1.0)/diff, avgResponse, minResponse, maxResponse)
                count = 0
                sumResponse = 0.0
                minResponse = 100000.0
                maxResponse = -1.0
                loopTime = nowTime
        except EOFError:
            nowTime = time.time()
            diff = nowTime - loopTime
            avgResponse = sumResponse / (count * 1.0) if count > 0 else sumResponse
            print "{0:.0f}\t{1:.3f}\t{2:.3f}\t{3:.3f}\t{4:.3f}".format(round(nowTime*1000-baseTime), (count*1.0)/diff, avgResponse, minResponse, maxResponse)
            break

    print "Total operations {0}".format(total)

################################################################################################

if __name__ == "__main__":
    parser = ArgumentParser(description="NoBench Workload Simulator")
    parser.add_argument("--db_url", type=str, help="Database connection url", required=True)
    parser.add_argument("--user", type=str, help="Database user", required=True)
    parser.add_argument("--passwd", type=str, default="", help="Database user password")
    parser.add_argument("--workers", type=int, default=1, help="Number of workers")
    parser.add_argument("--threads", type=int, default=1, help="Number of threads")
    parser.add_argument("--query", type=int, default=1, help="Number of query")
    parser.add_argument("--table", type=str, default="NOBENCH", help="Name of table")
    parser.add_argument("--recordcount", type=int, default=100, help="Name of ops per worker thread")
    parser.add_argument("--range", metavar="INT", type=int, nargs=2, help="Record range", required=True)
    parser.add_argument("--interval", type=int, default=10, help="Statistics interval")

    args = parser.parse_args()
    db_params = {
        "db_url": args.db_url,
        "user": args.user,
        "passwd": args.passwd,
        "table": args.table,
        "range": args.range
    }

    processArray = []

    out_p, in_p = Pipe(False)
    lock = Lock()

    for i in xrange(args.workers):
        proc = Process(target=runWorker, args=((out_p, in_p),lock,db_params,args.query,args.threads,args.recordcount))
        processArray.append(proc)
        proc.start()

    monitor((out_p, in_p), args.interval)

    for proc in processArray:
        proc.join()

    exit(0)
