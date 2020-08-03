from ast import literal_eval
import csv
import os
import sys
from typing import Any, Dict, List

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def get_experiment_files(out_dir: str) -> Dict[str, str]:
    return {block_file.split('_')[-1].split('.')[0]: block_file for block_file in os.listdir(out_dir)
            if os.path.isfile(os.path.join(out_dir, block_file))
            and 'blocks_time' in block_file}


def experiment_transactions(out_dir: str, exp_files: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    txs = {}
    reactions = {}
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
    return txs, reactions


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


def process_block_times(out_dir: str) -> None:
    # Get files to process in the experiment
    files = get_experiment_files(out_dir)
    # Transactions for the experiments
    txs, reacts = experiment_transactions(out_dir, files)
    df = create_data_frame(txs)
    latencies = get_latencies(df, reactions=reacts)

    sns.set(color_codes=True)
    output_dir = out_dir

    plot_tx_time_df(output_dir, df)
    plot_latency_df(output_dir, latencies)
    plot_throughput_raw(output_dir, df)
    plot_throughput_reaction(output_dir, df, reactions=reacts)


def run(out_dir: str) -> None:
    process_block_times(out_dir)


# cd to the output directory
os.chdir(os.environ['OUTPUT_DIR'])

run(sys.argv[1])
