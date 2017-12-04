#!/usr/bin/env python

import json
from argparse import ArgumentParser

def parseData(filename, rewrite=False):
    prefix = filename[:filename.rindex('.')]
    f = open(prefix+'.dat', 'r')
    if rewrite:
        g = open(prefix+'_parsed.dat', 'w+')
    g1 = open(prefix+'_lookup.dat', 'w+')
    g2 = open(prefix+'_lookup_text.dat', 'w+')

    lookup_values = {}
    nested_dict = {}
    sparse_dict = {}

    count = 0
    while True:
        line_in = f.readline()
        if line_in == "":
            break
        count += 1
        data = json.loads(line_in)
        
        lookup_values[data["num"]] = {
            "str1": data["str1"],
            "dyn1": data["dyn1"]
        }

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

    for v in lookup_values:
        l = "{0} {1} {2}\n".format(v, lookup_values[v]["str1"], lookup_values[v]["dyn1"])
        g1.write(l)

    line = " ".join(sparse_dict.keys())
    g2.write(line+"\n")
    
    line = " ".join(nested_dict.keys())
    g2.write(line)
    g2.write("\n")

    f.close()
    if rewrite: g.close()
    g1.close()
    g2.close()

    print count

######################################################################

if __name__ == "__main__":
    parser = ArgumentParser(description="Parse NoBench Data")
    parser.add_argument("--file", type=str, help="Input file", required=True)
    #parser.add_argument("--rewrite", type=bool, default=False, choices=[True, False], help="Rewrite Output")

    args = parser.parse_args()

    parseData(args.file)

