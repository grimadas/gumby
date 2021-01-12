#!/usr/bin/env python3
from collections import Counter, defaultdict
import logging
import os
import signal
import sys
import time

from scapy.all import AsyncSniffer


class ScapyMonitor:

    def __init__(self) -> None:
        self.packet_counts = Counter()
        self.bytes_counts = defaultdict(int)
        self.sniff_task = None

        self.packets = []

        print('Starting scapy')

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Starting scapy monitor", self.__class__.__name__)

    def update_stats(self, packet) -> None:
        # Create tuple of Src/Dst in sorted order
        try:
            key = (f"{packet[0][1].src}:{packet[0][1].sport}",
                   f"{packet[0][1].dst}:{packet[0][1].dport}")
            self.packet_counts.update([key])
            self.bytes_counts[key] += len(packet)

            self.packets.append((key[0], key[1], len(packet),
                                 int(round(time.time() * 1000)),
                                 packet.summary()))
        except AttributeError as _:
            pass

    def start(self) -> None:
        ip_filter = 'ip dst net 192.42.116.0/24 and src net 192.42.116.0/24'
        print('Start sniffing')
        self.sniff_task = AsyncSniffer(filter=ip_filter,
                                       prn=self.update_stats,
                                       iface=['eno5', 'lo']
                                       )
        self.sniff_task.start()

    def write_packet_counts(self, prefix) -> None:
        import csv
        csv_columns = ['src', 'dst', 'count']

        csv_file = os.path.join(prefix, "packet_counts.csv")
        try:
            with open(csv_file, 'w') as f:
                writer = csv.DictWriter(f, fieldnames=csv_columns)
                writer.writeheader()
                for k, val in self.packet_counts.items():
                    writer.writerow({'src': k[0], 'dst': k[1], 'count': val})
        except IOError:
            print("I/O error")

    def write_packets(self, prefix) -> None:
        import csv
        csv_columns = ['src', 'dst', 'len', 'time', 'sum']

        csv_file = os.path.join(prefix, "packet_time.csv")
        try:
            with open(csv_file, 'w') as f:
                writer = csv.DictWriter(f, fieldnames=csv_columns)
                writer.writeheader()
                for val in self.packets:
                    writer.writerow({'src': val[0], 'dst': val[1],
                                     'len': val[2], 'time': val[3],
                                     'sum': val[4]
                                     })
        except IOError:
            print("I/O error")

    def write_bytes_counts(self, prefix) -> None:
        import csv
        csv_columns = ['src', 'dst', 'count']

        csv_file = os.path.join(prefix, "scapy_bandwidth.csv")
        try:
            with open(csv_file, 'w') as f:
                writer = csv.DictWriter(f, fieldnames=csv_columns)
                writer.writeheader()
                for k, val in self.bytes_counts.items():
                    writer.writerow({'src': k[0], 'dst': k[1], 'count': val})
        except IOError:
            print("I/O error")

    def stop(self) -> None:
        print('Stopping sniffing')
        if self.sniff_task:
            self.sniff_task.stop()
            # Write stats to the file
            prefix = os.environ.get('PROJECT_DIR', '')
            prefix = ''
            self.write_packets(prefix)
            self.write_bytes_counts(prefix)
        sys.exit(0)


v = ScapyMonitor()


def signal_handler(_, __):
    print('Stopping scapy')
    v.logger.info('Stopping scapy monitor')
    v.stop()


v.start()
signal.signal(signal.SIGTERM, signal_handler)
signal.pause()
