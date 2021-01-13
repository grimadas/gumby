import pandas as pd

from gumby.statsparser import StatisticsParser


class BlockchainTransactionsParser(StatisticsParser):
    """
    This class parsers blockchain transactions.
    """

    def __init__(self, node_directory):
        super(BlockchainTransactionsParser, self).__init__(node_directory)
        self.transactions = []
        self.cumulative_stats = []
        self.avg_start_time = 0

        self.latency_moments = {}

    def parse(self):
        """
        Parse all blockchain statistics.
        """
        self.compute_avg_start_time()
        self.parse_transactions()
        self.compute_latency_stat_moments()
        self.compute_tx_cumulative_stats()
        self.combine_bandwidth_stats()
        self.aggregate_disk_usage()
        self.write_all()

    def compute_avg_start_time(self):
        avg_start_time = 0
        num_files = 0
        for _, filename, _ in self.yield_files('submit_tx_start_time.txt'):
            with open(filename) as submit_tx_start_time_file:
                start_time = int(submit_tx_start_time_file.read())
                avg_start_time += start_time
                num_files += 1

        self.avg_start_time = int(avg_start_time / num_files) if num_files > 0 else -1

    def combine_bandwidth_stats(self):
        with open('total_bandwidth.csv', 'w') as b_f:
            b_f.write('total_up,total_down,peer_nr\n')
            for peer_nr, filename, _ in self.yield_files('bandwidth.txt'):
                with open(filename, 'r') as annotation_file:
                    line = annotation_file.read()
                    b_f.write('%s,%d\n' % (line, int(peer_nr)))

        with open("system_bandwidth.csv", "w") as b_f:
            b_f.write("src,dst,count,peer_nr\n")
            for peer_nr, filename, _ in self.yield_files('system_bandwidth.csv'):
                first = True
                with open(filename, 'r') as annotation_file:
                    for line in annotation_file.readlines():
                        if first:
                            first = False
                            continue
                        b_f.write("%s,%d\n" % (line[:-1], int(peer_nr)))

        with open("scapy_bandwidth.csv", "w") as b_f:
            b_f.write("src,dst,count,peer_nr\n")
            for peer_nr, filename, _ in self.yield_files('scapy_bandwidth.csv'):
                first = True
                with open(filename, 'r') as annotation_file:
                    for line in annotation_file.readlines():
                        if first:
                            first = False
                            continue
                        b_f.write("%s,%d\n" % (line[:-1], int(peer_nr)))

    def parse_transactions(self):
        """
        This method should be implemented by the sub-class since it depends on the individual blockchain
        implementations. The execution of this method should fill the self.transactions array with information.
        """
        pass

    def compute_latency_stat_moments(self):
        v = [t[4] if t[4] >= 0 else None for t in self.transactions]
        self.latency_moments = pd.Series(v).dropna().describe().to_dict()

    def compute_tx_cumulative_stats(self):
        """
        Compute cumulative transaction statistics.
        """
        submit_times = []
        confirm_times = []
        for transaction in self.transactions:
            submit_times.append(transaction[2])
            if transaction[3] != -1:
                confirm_times.append(transaction[3])

        submit_times = sorted(submit_times)
        confirm_times = sorted(confirm_times)

        cumulative_window = 100  # milliseconds
        cur_time = 0
        submitted_tx_index = 0
        confirmed_tx_index = 0

        submitted_count = 0
        confirmed_count = 0
        self.cumulative_stats = [(0, 0, 0)]

        if not submit_times or not confirm_times:
            return

        while cur_time < max(submit_times[-1], confirm_times[-1]):
            # Increase counters
            while submitted_tx_index < len(submit_times) and \
                    submit_times[submitted_tx_index] <= cur_time + cumulative_window:
                submitted_tx_index += 1
                submitted_count += 1

            while confirmed_tx_index < len(confirm_times) and \
                    confirm_times[confirmed_tx_index] <= cur_time + cumulative_window:
                confirmed_tx_index += 1
                confirmed_count += 1

            cur_time += cumulative_window
            self.cumulative_stats.append((cur_time, submitted_count, confirmed_count))

    def aggregate_disk_usage(self):
        """
        Aggregate the disk usage of individual nodes
        """
        with open("disk_usage.csv", "w") as disk_usage_file:
            disk_usage_file.write("peer_id,disk_usage\n")
            for peer_nr, filename, _ in self.yield_files('disk_usage.txt'):
                with open(filename, "r") as individual_disk_usage_file:
                    disk_usage = int(individual_disk_usage_file.read())
                    disk_usage_file.write("%d,%d\n" % (peer_nr, disk_usage))

    def write_all(self):
        """
        Write all information to disk.
        """
        with open("transactions.txt", "w") as transactions_file:
            transactions_file.write("peer_id,tx_id,submit_time,confirm_time,latency\n")
            for transaction in self.transactions:
                transactions_file.write("%d,%s,%d,%d,%d\n" % transaction)

        with open("tx_cumulative.csv", "w") as out_file:
            out_file.write("time,submitted,confirmed\n")
            for result in self.cumulative_stats:
                out_file.write("%d,%d,%d\n" % result)

        with open("latency.txt", "w") as latency_file:
            latency_file.write(str(self.latency_moments))
