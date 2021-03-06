#!/usr/bin/env python
from __future__ import print_function
import sys
import os
from math import ceil
from collections import defaultdict

def float_or_unknown(float_str):
    return float_str if float_str == "?" else float(float_str)

def float_or_unknown_mean(parts):
    if all(float_str == "?" for float_str in parts):
        return "?"

    not_unknowns = sum(float(float_str) for float_str in parts if float_str != "?")
    return not_unknowns / float(len(parts))

def reduce(base_directory, nrlines, inputfile, outputfile, removeinputfile=True):
    inputfile = os.path.join(base_directory, inputfile)
    outputfile = os.path.join(base_directory, outputfile)

    if os.path.exists(inputfile):
        print(base_directory, inputfile, outputfile, file=sys.stderr)

        ifp = open(inputfile, 'r')
        ofp = open(outputfile, 'w')

        lines = ifp.readlines()
        print(lines[0][:-1], file=ofp)

        lines = lines[1:]
        if len(lines) > nrlines:
            nrlines_to_merge = int(ceil(len(lines) / float(nrlines)))
            print("%s has %d lines, reducing to %d lines" % (inputfile, len(lines), nrlines), file=sys.stderr)

            max_time = None
            to_be_merged_parts = defaultdict(list)
            for i, line in enumerate(lines):
                parts = line.split()
                max_time = max(float(parts[0]), max_time)

                parts = map(float_or_unknown, parts[1:])
                for j, part in enumerate(parts):
                    to_be_merged_parts[j].append(part)

                if (i + 1) % nrlines_to_merge == 0 or (i + 1 == len(lines)):
                    print(max_time, end=' ', file=ofp)

                    for j, parts in to_be_merged_parts.items():
                        mean = float_or_unknown_mean(parts)
                        print(mean, end=' ', file=ofp)

                        to_be_merged_parts[j] = []

                    print('', file=ofp)

        else:
            for line in lines:
                print(line[:-1], file=ofp)

        ifp.close()
        ofp.close()

        if removeinputfile:
            os.remove(inputfile)

def main(input_directory, nrlines):
    for filename in ['send', 'send_diff', 'received', 'received_diff', 'dropped', 'dropped_diff', 'bl_skip', 'bl_skip_diff', 'bl_reuse', 'bl_reuse_skip', 'utimes', 'stimes', 'wchars', 'rchars', 'writebytes', 'readbytes', 'vsizes', 'rsizes', 'utimes_node', 'stimes_node', 'wchars_node', 'rchars_node', 'writebytes_node', 'readbytes_node', 'vsizes_node', 'rsizes_node', 'sum_total_records', 'sum_statistics']:
        reduce(input_directory, nrlines, '%s.txt' % filename, '%s_reduced.txt' % filename)

    total_communities = 1
    while os.path.exists(os.path.join(input_directory, 'total_connections_%d.txt' % total_communities)):
        reduce(input_directory, nrlines, 'total_connections_%d.txt' % total_communities, 'total_connections_%d_reduced.txt' % total_communities)
        reduce(input_directory, nrlines, 'total_walked_%d.txt' % total_communities, 'total_walked_%d_reduced.txt' % total_communities)
        reduce(input_directory, nrlines, 'total_stumbled_%d.txt' % total_communities, 'total_stumbled_%d_reduced.txt' % total_communities)
        reduce(input_directory, nrlines, 'total_intro_%d.txt' % total_communities, 'total_intro_%d_reduced.txt' % total_communities)
        reduce(input_directory, nrlines, 'sum_incomming_connections_%d.txt' % total_communities, 'sum_incomming_connections_%d_reduced.txt' % total_communities)

        reduce(input_directory, nrlines, 'bl_reuse_%d.txt' % total_communities, 'bl_reuse_%d_reduced.txt' % total_communities)
        reduce(input_directory, nrlines, 'bl_skip_%d.txt' % total_communities, 'bl_skip_%d_reduced.txt' % total_communities)
        reduce(input_directory, nrlines, 'bl_time_%d.txt' % total_communities, 'bl_time_%d_reduced.txt' % total_communities)

        total_communities += 1

    for filename in os.listdir(input_directory):
        if filename.endswith('-debugstatistics.txt'):
            reduce(input_directory, nrlines, filename, filename[:-4] + '_reduced.txt')

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: %s <peers-directory> <nr-of-lines-to-output>" % (sys.argv[0]))
        print(sys.argv, file=sys.stderr)

        exit(1)

    main(sys.argv[1], int(sys.argv[2]))
