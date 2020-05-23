#!/usr/bin/env python
from __future__ import print_function

import csv
import json
import os
import statistics as np
import sys
from binascii import hexlify

from gumby.post_process_blockchain import BlockchainTransactionsParser

from scripts.trustchain_database_reader import GumbyDatabaseAggregator


class PlexusStatisticsParser(BlockchainTransactionsParser):
    """
    Parse Noodle statistics after an experiment has been completed.
    """

    def __init__(self, node_directory):
        super(PlexusStatisticsParser, self).__init__(node_directory)
        self.aggregator = GumbyDatabaseAggregator(os.path.join(os.environ['PROJECT_DIR'], 'output'))
        self.tx_propagation_info = {}  # Keep track of whether a transaction has been seen by the counterparty / confirmed by the initiator

    def aggregate_databases(self):
        aggregation_path = os.path.join(os.environ['PROJECT_DIR'], 'output', 'sqlite')
        if not os.path.exists(aggregation_path):
            os.makedirs(aggregation_path)

        self.aggregator.combine_databases()

    def parse_transactions(self):
        """
        Parse all transactions, based on the info in the blocks.
        """
        with open("blocks.csv", "w") as blocks_file:
            writer = csv.DictWriter(blocks_file, ['seen_by', 'time', 'transaction', 'type', 'peer_id',
                                                  "seq_num", 'com_id', 'com_seq', "links", 'prevs'])
            writer.writeheader()

            for peer_nr, filename, dir in self.yield_files('blocks.csv'):
                with open(filename) as read_file:
                    csv_reader = csv.DictReader(read_file)
                    for row in csv_reader:
                        writer.writerow({
                            'seen_by': peer_nr,
                            "time": row['time'],
                            'transaction': row['transaction'],
                            'type': row['type'],
                            'seq_num': row['seq_num'],
                            'peer_id': row['peer_id'],
                            'com_id': row['com_id'],
                            "com_seq": row['com_seq'],
                            'links': row['links'],
                            'prevs': row['prevs']})

    def parse_states(self):
        with open("states_comb.csv", "w") as blocks_file:
            writer = csv.DictWriter(blocks_file, ['seen_by', 'chain_id', 'last_state', 'personal'])
            writer.writeheader()

            for peer_nr, filename, dir in self.yield_files('states.csv'):
                with open(filename) as read_file:
                    csv_reader = csv.DictReader(read_file)
                    for row in csv_reader:
                        writer.writerow({
                            'seen_by': peer_nr,
                            "chain_id": row['chain_id'],
                            "last_state": row['last_state'],
                            "personal": row['personal']})

    def run(self):
        self.parse_transactions()
        self.parse_states()


if __name__ == "__main__":
    # cd to the output directory
    # cd to the output directory
    os.chdir(os.environ['OUTPUT_DIR'])

    parser = PlexusStatisticsParser(sys.argv[1])
    parser.run()
