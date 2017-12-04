import time
from argparse import ArgumentParser, Action
from multiprocessing import Process, Pipe, Lock
from nobench_worker import NoBenchWorker

def runWorker(pipe, lock, db_params, queryType, threads, recordcount, lookup_values, lookup_values_text) :
    worker = NoBenchWorker(pipe, lock, db_params, queryType, threads, recordcount, lookup_values, lookup_values_text)
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
            if diff == 0: diff+=1
            avgResponse = sumResponse / (count * 1.0) if count > 0 else sumResponse
            print "{0:.0f}\t{1:.3f}\t{2:.3f}\t{3:.3f}\t{4:.3f}".format(round(nowTime*1000-baseTime), (count*1.0)/diff, avgResponse, minResponse, maxResponse)
            break

    print "Total operations {0}".format(total)

################################################################################################

#class LoadFromFile (Action):
#    def __call__ (self, parser, namespace, values, option_string = None):
#        with values as f:
#            parser.parse_args(f.read().split(), namespace)

if __name__ == "__main__":
    parser = ArgumentParser(description="NoBench Workload Simulator")
    #parser.add_argument('--params', metavar="FILE", type=open, action=LoadFromFile, help="Load params from file")
    parser.add_argument("--db_url", metavar="STR", type=str, help="Database connection url", required=False)
    parser.add_argument("--user", metavar="STR", type=str, help="Database user", required=False)
    parser.add_argument("--passwd", metavar="STR", type=str, default="", help="Database user password")
    parser.add_argument("--workers", metavar="INT", type=int, default=1, help="Number of workers")
    parser.add_argument("--threads", metavar="INT", type=int, default=1, help="Number of threads")
    parser.add_argument("--query", metavar="INT", type=int, default=1, choices=xrange(1, 13), help="Number of query")
    parser.add_argument("--table", metavar="STR", type=str, default="NOBENCH", help="Name of table")
    parser.add_argument("--recordcount", metavar="INT", type=int, default=100, help="Number of ops per worker thread")
    parser.add_argument("--range", metavar="INT", type=int, nargs=2, help="Record range", required=False)
    parser.add_argument("--interval", metavar="INT", type=int, default=10, help="Statistics interval")
    parser.add_argument("--lookup", metavar="FILE", type=str, help="str1 and dyn1 values")
    parser.add_argument("--lookup_text", metavar="FILE", type=str, help="sparse and nested array values")

    args = parser.parse_args()
    db_params = {
        "db_url": args.db_url,
        "user": args.user,
        "passwd": args.passwd,
        "table": args.table,
        "range": args.range
    }

    lookup_values = None
    if args.lookup is not None:
        lookup_values = {}
        f = open(args.lookup, "r")
        lines = f.read().splitlines()
        for line in lines:
            l = line.split()
            lookup_values[int(l[0])] = {"str1": l[1], "dyn1": l[2]}
            #lookup_values[int(l[0])] = [l[1], l[2]]
        f.close()
    
    lookup_values_text = None
    if args.lookup_text is not None:
        f = open(args.lookup_text, "r")
        lines = f.read().splitlines()
        sparse_options = lines[0].split()
        nested_options = lines[1].split()
        lookup_values_text = {
            "nested_options": nested_options,
            "nested_options_len": len(nested_options),
            "sparse_options": sparse_options,
            "sparse_options_len": len(sparse_options)
        }
        f.close()

    processArray = []
    out_p, in_p = Pipe(False)
    lock = Lock()

    for i in xrange(args.workers):
        proc = Process(target=runWorker, args=((out_p, in_p),lock,db_params,args.query,args.threads,args.recordcount, lookup_values, lookup_values_text))
        processArray.append(proc)
        proc.start()

    monitor((out_p, in_p), args.interval)

    for proc in processArray:
        proc.join()

    exit(0)
