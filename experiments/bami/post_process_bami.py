#!/usr/bin/env python3
from ast import literal_eval
import csv
import os
import sys
from typing import Any, Dict, List

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np

from gumby.statsparser import StatisticsParser


def get_experiment_files(out_dir: str) -> Dict[str, str]:
    return {block_file.split('_')[-1].split('.')[0]: block_file for block_file in os.listdir(out_dir)
            if os.path.isfile(os.path.join(out_dir, block_file))
            and 'blocks_time' in block_file}


def experiment_transactions(out_dir: str, exp_files: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    txs = {}
    reactions = {}
    types = {}
    for p_id, f in exp_files.items():
        full_path = os.path.join(out_dir, f)
        with open(full_path) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # creator = row['creator']
                tx_id = str(row['group_id']) + str(row['dot'])
                tx_type = row['type']
                tx_time = row['time']

                if tx_id not in txs:
                    txs[tx_id] = {}
                txs[tx_id][p_id] = float(tx_time)
                # Process reaction
                if tx_type == "b'confirm'" or tx_type == "b'reject'":
                    tx_val = literal_eval(row['transaction'])
                    linked_tx = str(row['group_id']) + str(tx_val[b'dot'])
                    # Remember link
                    reactions[tx_id] = linked_tx
                types[tx_id] = tx_type
    return txs, reactions, types


def get_latencies(df: pd.DataFrame, reactions: Dict) -> List[float]:
    return [df[[r_tx]].min()[0] - df[[o_tx]].min()[0] for r_tx, o_tx in reactions.items()]


def create_data_frame(transactions: Dict) -> pd.DataFrame:
    return pd.DataFrame(transactions)


def plot_tx_time_df(out_dir: str, df: pd.DataFrame) -> None:
    """Save latency plots to the out_dir"""
    plt.figure()
    tx_diff = df.transform(lambda x: x - x.min()).values
    ax = sns.distplot(tx_diff)
    ax.set_title('Received time distribution')
    ax.set_xlabel('Time (seconds)')
    save_path = os.path.join(out_dir, "time_pdf.png")
    plt.savefig(save_path)

    # Save cdf
    plt.figure()
    bx = sns.distplot(tx_diff, hist_kws={'cumulative': True, 'density': True}, kde_kws={'cumulative': True})
    bx.set_title('Received time CDF')
    bx.set_xlabel('Time (seconds)')
    save_path = os.path.join(out_dir, "time_cdf.png")
    plt.savefig(save_path)


def plot_latency_df(out_dir: str, latencies: List[float]) -> None:
    """Save latency plots to the out_dir"""
    plt.figure()
    ax = sns.distplot(latencies)
    ax.set_title('Latency distribution')
    ax.set_xlabel('Time (seconds)')
    save_path = os.path.join(out_dir, "latency_df.png")
    ax.get_figure().savefig(save_path)


def plot_throughput_raw(out_dir: str, df: pd.DataFrame) -> None:
    plt.figure()
    ax = sns.distplot(df.mean().values, bins=10, kde=False, rug=True)
    ax.set_title('Raw histogram of mean time received')
    ax.set_xlabel('Time since start (seconds)')
    save_path = os.path.join(out_dir, "throughput_raw.png")
    ax.get_figure().savefig(save_path)


def plot_throughput_reaction(out_dir: str, df: pd.DataFrame, reactions: Dict) -> None:
    plt.figure()
    received_react = [df[[r_tx]].min()[0] for r_tx, o_tx in reactions.items()]
    ax = sns.distplot(received_react, bins=10, kde=False, rug=True)
    ax.set_title('Histogram of mean time reaction received')
    ax.set_xlabel('Time since start (seconds)')
    save_path = os.path.join(out_dir, "throughput_reaction.png")
    ax.get_figure().savefig(save_path)


def plot_number_of_blocks(out_dir: str, df: pd.DataFrame, types: Dict) -> None:
    df2 = df.reset_index().melt(id_vars=['index']).dropna()
    df2['type'] = df2.variable.transform(lambda x: types[x])
    plt.figure()
    g = sns.countplot(data=df2, x='index', hue='type')

    # for p in g.patches:
    #    annotate_text = "{:.2f}".format(p.get_height())
    #    g.annotate(annotate_text, (p.get_x() + p.get_width() / 2., p.get_height()),
    #               ha='center', va='center', fontsize=11, color='#444', xytext=(0, 20),
    #               textcoords='offset points')

    # _ = g.set_ylim(0, max(p.get_height() + 5 for p in g.patches))  # To make space for the annotations

    g.set_title('Number of blocks finalized by peer')
    g.set_xlabel('Peer ID')
    save_path = os.path.join(out_dir, "num_blocks_by_peer.png")
    g.get_figure().savefig(save_path)


def process_block_times(out_dir: str) -> None:
    # Get files to process in the experiment
    print('Processing block times on dir', out_dir)
    files = get_experiment_files(out_dir)
    print('Total number of block files', len(files))
    # Transactions for the experiments
    txs, reacts, types = experiment_transactions(out_dir, files)
    df = create_data_frame(txs)
    latencies = get_latencies(df, reactions=reacts)

    sns.set(color_codes=True)
    output_dir = out_dir

    plot_tx_time_df(output_dir, df)
    plot_latency_df(output_dir, latencies)
    plot_throughput_raw(output_dir, df)
    plot_throughput_reaction(output_dir, df, reactions=reacts)
    plot_number_of_blocks(output_dir, df, types)


def plot_peer_bandwidth(out_dir: str, df: pd.DataFrame):
    median = df.val.median()

    def plot_mean(*args, **kwargs):
        plt.axhline(median, *args, **kwargs)

    plt.figure()
    g = sns.FacetGrid(df, col="peer", aspect=1., col_wrap=4, height=3, )

    g.fig.subplots_adjust(top=0.9)
    g.fig.suptitle('Bandwidth by peer', fontsize=16)

    g.map(sns.barplot, 'type', 'val', order=['up', 'down'])
    g.map(plot_mean, ls=":", c=".3")
    g.set_axis_labels(x_var='', y_var='Bandwidth (MB)')
    save_path = os.path.join(out_dir, 'peer_bandwidth.png')
    plt.savefig(save_path)


def plot_total_bandwidth(out_dir: str, total_up: float, total_down: float) -> None:
    lost = total_up - total_down
    plt.figure()
    g = sns.barplot(['total_up', 'total_down', 'lost'], [total_up, total_down, lost])

    i = 0
    for p in g.patches:
        annotate_text = "{:.2f} ({:.1f}%)".format(p.get_height(), 100 * lost / total_up) \
            if i == 2 else "{:.2f}".format(p.get_height())
        g.annotate(annotate_text, (p.get_x() + p.get_width() / 2., p.get_height()),
                   ha='center', va='center', fontsize=11, color='gray', xytext=(0, 20),
                   textcoords='offset points')
        i += 1

    _ = g.set_ylim(0, max(p.get_height() + 10 for p in g.patches))  # To make space for the annotations
    g.set_title('Total system bandwidth')
    g.set_ylabel('Bandwidth (MB)')
    save_path = os.path.join(out_dir, 'total_bandwidth.png')
    plt.savefig(save_path)


def process_bandwidth_files(out_dir: str) -> None:
    parser = StatisticsParser(out_dir)
    total_up, total_down = 0, 0
    uploads = []
    downs = []
    peers = []

    for peer_nr, filename, dir in parser.yield_files('bandwidth.txt'):
        with open(filename) as bandwidth_file:
            parts = bandwidth_file.read().rstrip('\n').split(",")
            u, d = int(parts[0]) / (2 ** 20), int(parts[1]) / (2 ** 20)

            uploads.append(u)
            downs.append(d)
            peers.append(peer_nr)

            total_up += u
            total_down += d
    # Prepare bandwidth table
    df = pd.DataFrame({'peer': np.array(peers),
                       'up': np.array(uploads),
                       'down': np.array(downs)})
    df = pd.melt(df, id_vars="peer", var_name="type", value_name="val")
    plot_peer_bandwidth(out_dir, df)
    plot_total_bandwidth(out_dir, total_up, total_down)


def run(out_dir: str) -> None:
    process_block_times(out_dir)
    process_bandwidth_files(out_dir)


# cd to the output directory
os.chdir(os.environ['OUTPUT_DIR'])

run(sys.argv[1])
