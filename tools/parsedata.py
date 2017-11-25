#!/usr/bin/env python

import json
from argparse import ArgumentParser

def parseData(filename, rewrite=False):
    prefix = filename[:filename.rindex('.')]
    f = open(prefix+'.dat', 'r')
    if rewrite:
        g = open(prefix+'_parsed.dat', 'w+')
    g1 = open(prefix+'_stats_dyn1.dat', 'w+')
    g2 = open(prefix+'_stats_str1.dat', 'w+')
    g3 = open(prefix+'_stats_nested.dat', 'w+')
    g4 = open(prefix+'_stats_sparse.dat', 'w+')

    dyn1_dict = {}
    str1_dict = {}
    nested_dict = {}
    sparse_dict = {}

    count = 0
    while True:
        line_in = f.readline()
        if line_in == "":
            break
        count += 1
        data = json.loads(line_in)
        
        if data["str1"] not in str1_dict:
            str1_dict[data["str1"]] = 1

        if str(data["dyn1"]) not in dyn1_dict:
            dyn1_dict[str(data["dyn1"])] = 1

        for e in data["nested_arr"]:
            if e not in nested_dict:
                nested_dict[e] = 1
        
        values = [ v for k,v in data.items() if 'sparse' in k]
        for e in values:
            if e not in sparse_dict:
                sparse_dict[e] = 1

        if rewrite:
            line_out = "{0}, {1}\n".format(str(data["num"]), json.dumps(data))
            g.write(line_out)

    for d in dyn1_dict:
        g1.write(d+"\n")

    for s in str1_dict:
        g2.write(s+"\n")

    for item in nested_dict:
        g3.write(item+"\n")
        #g3.write(item + "\t" + str(nested_dict[item]) + "\n")
    
    for s in sparse_dict:
        g4.write(s+"\n")

    f.close()
    if rewrite: g.close()
    g1.close()
    g2.close()
    g3.close()
    g4.close()

    print count

######################################################################

if __name__ == "__main__":
    parser = ArgumentParser(description="Parse NoBench Data")
    parser.add_argument("--file", type=str, help="Input file", required=True)
    #parser.add_argument("--rewrite", type=bool, default=False, choices=[True, False], help="Rewrite Output")

    args = parser.parse_args()

    parseData(args.file)

