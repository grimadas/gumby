#!/usr/bin/env python
from __future__ import print_function

import csv
import json
import os
import sys

from gumby.statsparser import StatisticsParser

from Tribler.Core.Utilities.unicode import hexlify

from scripts.trustchain_database_reader import GumbyDatabaseAggregator


class TrustchainStatisticsParser(StatisticsParser):
    """
    Parse TrustChain statistics after an experiment has been completed.
    """

    def __init__(self, node_directory):
        super(TrustchainStatisticsParser, self).__init__(node_directory)
        self.aggregator = GumbyDatabaseAggregator(os.path.join(os.environ['PROJECT_DIR'], 'output'))
        self.do_cleanup = False
        if os.getenv('CLEAN_UP'):
            self.do_cleanup = bool(os.getenv('CLEAN_UP'))

    def aggregate_databases(self):
        aggregation_path = os.path.join(os.environ['PROJECT_DIR'], 'output', 'sqlite')
        if not os.path.exists(aggregation_path):
            os.makedirs(aggregation_path)

        self.aggregator.combine_databases()

    def write_blocks_to_file(self):
        # First, determine the experiment start time
        start_time = 0
        for peer_nr, filename, dir in self.yield_files('start_time.txt'):
            with open(filename) as start_time_file:
                start_time = int(float(start_time_file.read()) * 1000)
                break

        print("Writing TrustChain blocks to file")
        # Prior to writing all blocks, we construct a map from peer ID to public key
        key_map = {}
        for peer_nr, filename, dir in self.yield_files('overlays.txt'):
            with open(filename) as overlays_file:
                content = overlays_file.readlines()
                for line in content:
                    if not line:
                        continue
                    parts = line.split(',')
                    if parts[0] == 'TrustChainCommunity':
                        print("Mapping %s to peer %s" % (parts[1].rstrip(), peer_nr))
                        key_map[parts[1].rstrip()] = peer_nr

        interactions = []

        # Get all blocks
        blocks = self.aggregator.database.get_all_blocks()

        with open('trustchain.csv', 'w') as trustchain_file:
            # Write header
            trustchain_file.write(
                "peer;public_key;sequence_number;link_peer;link_public_key;"
                "link_sequence_number;previous_hash;signature;hash;type;time;time_since_start;tx\n"
            )

            # Write blocks
            for block in blocks:
                if hexlify(block.link_public_key) not in key_map:
                    link_peer = 0
                else:
                    link_peer = key_map[hexlify(block.link_public_key)]

                if hexlify(block.public_key) not in key_map:
                    print("Public key %s cannot be mapped to a peer!" % hexlify(block.public_key))
                    continue

                peer = key_map[hexlify(block.public_key)]
                trustchain_file.write(
                    "%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%d;%s;%s\n" % (
                        peer,
                        hexlify(block.public_key),
                        block.sequence_number,
                        link_peer,
                        hexlify(block.link_public_key),
                        block.link_sequence_number,
                        hexlify(block.previous_hash),
                        hexlify(block.signature),
                        hexlify(block.hash),
                        block.type,
                        block.timestamp,
                        block.timestamp - start_time,
                        json.dumps(block.transaction))
                )

                if (peer, link_peer) not in interactions and (link_peer, peer) not in interactions:
                    interactions.append((peer, link_peer))

        with open('trustchain_interactions.csv', 'w') as trustchain_interactions_file:
            trustchain_interactions_file.write("peer_a,peer_b\n")
            for peer_a, peer_b in interactions:
                trustchain_interactions_file.write("%d,%d\n" % (peer_a, peer_b))

    def aggregate_trustchain_balances(self):
        with open('trustchain_balances.csv', 'w') as balances_file:
            balances_file.write('peer,total_up,total_down,balance\n')
            for peer_nr, filename, dir in self.yield_files('trustchain.txt'):
                with open(filename) as tc_file:
                    tc_json = json.loads(tc_file.read())
                    total_up = tc_json['total_up']
                    total_down = tc_json['total_down']
                    balance = total_up - total_down
                    balances_file.write('%s,%d,%d,%d\n' % (peer_nr, total_up, total_down, balance))

    def aggregate_trustchain_times(self):
        prefix = os.path.join(os.environ['PROJECT_DIR'], 'output')
        postfix = 'leader_blocks_time_'
        index = 1

        agg_block_time = {}
        final_time = {}

        while os.path.exists(os.path.join(prefix, postfix + str(index) + '.csv')):
            with open(os.path.join(prefix, postfix + str(index) + '.csv')) as read_file:
                csv_reader = csv.reader(read_file)
                first = True
                for row in csv_reader:
                    if first:
                        first = False
                    else:
                        block_id = row[0]
                        time = row[1]
                        if block_id not in agg_block_time:
                            agg_block_time[block_id] = {}
                        agg_block_time[block_id][index] = time
                if self.do_cleanup:
                    os.remove(os.path.join(prefix, postfix + str(index) + '.csv'))
                index += 1

        stats = []
        nums = []

        for block_id in agg_block_time:
            b_id, seq, l_id, l_seq = block_id
            if l_seq != 0:
                # This is confirmation block
                tx_id = (l_id, l_seq)
                start_time = min(agg_block_time[(l_id, l_seq, b_id, 0)].values())
                end_time = min(agg_block_time[block_id].values())
                if tx_id not in final_time:
                    final_time[tx_id] = (end_time-start_time, end_time)

            tx_times = agg_block_time[block_id].values()
            start = min(tx_times)
            end = max(tx_times)
            num = len(tx_times)
            stats.append(end - start)
            nums.append(num)

        # Write the statistics for the files
        import math
        import statistics as np

        total_run = 60
        if os.getenv('TOTAL_RUN'):
            total_run = float(os.getenv('TOTAL_RUN'))
        throughput = {l: 0 for l in range(int(total_run+2) + 1)}
        latencies = []
        for tx_id in final_time:
            throughput[math.floor(final_time[tx_id][1])] += 1
            latencies.append(final_time[tx_id][0])

        # Time till everyone recieves

        # Write performance results in a file
        res_file = os.path.join(prefix, "perf_results.txt")
        with open(res_file, 'w') as w_file:
            w_file.write("Total operations: %d\n" % len(stats))
            w_file.write("Number of peers: %d\n" % max(nums))
            w_file.write("\n")

            if os.getenv('TX_SEC'):
                value = float(os.getenv('TX_SEC'))
                w_file.write("System transaction rate: %d\n" % (max(nums) * value))
            if os.getenv('IB_FANOUT'):
                value = int(os.getenv('IB_FANOUT'))
                w_file.write("Peer fanout: %d\n" % value)

            w_file.write("Peak throughput: %d\n" % max(throughput.values()))
            w_file.write("Avg throughput: %d\n" % np.mean(throughput.values()))
            w_file.write("St dev throughput: %d\n" % np.stdev(throughput.values()))
            w_file.write("Min throughput: %d\n" % min(throughput.values()))
            w_file.write("\n")

            w_file.write("Min round latency: %f\n" % min(latencies))
            w_file.write("Mean round latency: %f\n" % np.mean(latencies))
            w_file.write("St dev round latency: %f\n" % np.stdev(latencies))
            w_file.write("Max round latency: %f\n" % max(latencies))
            w_file.write("\n")

            w_file.write("Time for all to recieve: %f\n" % min(stats))
            w_file.write("Mean Time for all to recieve: %f\n" % np.mean(stats))
            w_file.write("St dev Time for all to recieve: %f\n" % np.stdev(stats))
            w_file.write("Max Time for all to recieve: %f\n" % max(stats))
            w_file.write("\n")

            w_file.write("Count max vals: %f\n" % nums.count(max(nums)))

    def run(self):
        self.aggregate_trustchain_times()
        self.aggregate_databases()
        self.write_blocks_to_file()
        self.aggregate_trustchain_balances()


# cd to the output directory
os.chdir(os.environ['OUTPUT_DIR'])

parser = TrustchainStatisticsParser(sys.argv[1])
parser.run()
