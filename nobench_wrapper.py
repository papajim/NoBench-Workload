from argparse import ArgumentParser
from multiprocessing import Process, Pipe, Lock
from nobench_worker import NoBenchWorker

def runWorker(pipe, lock, db_params, queryType, threads, recordcount) :
    worker = NoBenchWorker(pipe, lock, db_params, queryType, threads, recordcount)
    worker.start()

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

    in_p.close()
    count = 0
    while True:
        try:
            r = out_p.recv()
            count += r
        except EOFError:
            print count
            break

    for proc in processArray:
        proc.join()

    exit(0)
