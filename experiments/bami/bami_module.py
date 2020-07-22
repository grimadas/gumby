import csv
from decimal import Decimal
import os
from time import time
from typing import Optional

from bami.backbone.datastore.database import ChainTopic
from bami.backbone.utils import decode_raw
from bami.payment.community import PaymentCommunity
from bami.payment.exceptions import InsufficientBalanceException

from gumby.experiment import experiment_callback
from gumby.modules.community_experiment_module import IPv8OverlayExperimentModule
from gumby.modules.experiment_module import static_module


@static_module
class BamiExperiments(IPv8OverlayExperimentModule):
    def __init__(self, experiment):
        super().__init__(experiment, PaymentCommunity)
        self._logger.info('Creating experiment session: bami')
        self.request_signatures_lc = None
        self.num_blocks_in_db_task = None
        self.block_stat_file = None
        self.request_signatures_task = None

    @property
    def overlay(self) -> Optional[PaymentCommunity]:
        return super().overlay()

    def on_ipv8_available(self, _):
        # Disable threadpool messages
        self.overlay._use_main_thread = True
        self.overlay.ipv8 = self.ipv8

        self.change_settings_from_environ()

    def get_peer_public_key(self, peer_id):
        # override the default implementation since we use the trustchain key here.
        return self.all_vars[peer_id]['trustchain_public_key']

    def get_peer_id_by_pubkey(self, pub_key: bytes) -> Optional[str]:
        for p_id in self.all_vars:
            if pub_key == self.all_vars[p_id]['trustchain_public_key']:
                return p_id
        else:
            return None

    @experiment_callback
    def join_group(self, group_experiment_id: str) -> None:
        """Peer creates a sub-community with own id as the manager peer"""
        group_id = self.get_peer_public_key(group_experiment_id)
        print('Joining sub-community with id ', group_id)
        self.overlay.subscribe_to_subcom(group_id)

    @experiment_callback
    def mint(self, value: str = None) -> None:
        context = self.overlay.context
        # Change to default
        if not value:
            value = Decimal(1000, context)
        else:
            value = Decimal(value, context)
        self.overlay.mint(value=value)
        print("Mint request performed!")

    @experiment_callback
    def transfer(self, group_experiment_id: str, counter_party_id: str, value: str):
        context = self.overlay.context
        group_id = self.get_peer_public_key(group_experiment_id)
        counter_party_key_id = self.get_peer_public_key(counter_party_id)
        value = Decimal(value, context)
        try:
            self.overlay.spend(group_id, counter_party_key_id, value=value)
        except InsufficientBalanceException as e:
            print("Balance is not sufficient ", e)

    @experiment_callback
    def change_settings_from_environ(self):
        if os.environ.get('WITNESS_BLOCK_DELTA'):
            self.overlay.settings.witness_block_delta = float(os.environ.get('WITNESS_BLOCK_DELTA'))
        if os.environ.get('WITNESS_DELTA_TIME'):
            self.overlay.settings.witness_delta_time = float(os.environ.get('WITNESS_DELTA_TIME'))
        if os.environ.get('PUSH_GOSSIP_FANOUT'):
            self.overlay.settings.push_gossip_fanout = os.environ.get('PUSH_GOSSIP_FANOUT')
        if os.environ.get('PUSH_GOSSIP_TTL'):
            self.overlay.settings.push_gossip_ttl = os.environ.get('PUSH_GOSSIP_TTL')
        if os.environ.get('GOSSIP_MAX_DELAY'):
            self.overlay.settings.gossip_sync_max_delay = os.environ.get('GOSSIP_MAX_DELAY')
        if os.environ.get('GOSSIP_DELTA_TIME'):
            self.overlay.settings.gossip_sync_time = os.environ.get('GOSSIP_DELTA_TIME')
        if os.environ.get('GOSSIP_COLLECT_TIME'):
            self.overlay.settings.gossip_collect_time = os.environ.get('GOSSIP_COLLECT_TIME')
        if os.environ.get('BLOCK_SIGN_DELTA_TIME'):
            self.overlay.settings.block_sign_delta = os.environ.get('BLOCK_SIGN_DELTA_TIME')
        if os.environ.get('BLOCK_SIGN_DELTA_TIME'):
            self.overlay.settings.block_sign_delta = os.environ.get('BLOCK_SIGN_DELTA_TIME')
        if os.environ.get('BLOCK_MAX_WAIT_TIME'):
            self.overlay.settings.max_wait_time = os.environ.get('BLOCK_MAX_WAIT_TIME')
        if os.environ.get('PULL_GOSSIP_FANOUT'):
            self.overlay.settings.gossip_fanout = os.environ.get('PULL_GOSSIP_FANOUT')

    @experiment_callback
    def track_all_blocks(self):
        print('Tracking all blocks')
        # Open projects output directory and save blocks arrival time
        block_dict = ['time', 'group_id', 'creator', 'type', 'dot', 'transaction']

        def add_block(chain_id, dots):
            if not self.start_time:
                # First block received
                self.start_time = time()
            with open(self.block_stat_file, "a") as t_file:
                for dot in dots:
                    block = self.overlay.get_block_by_dot(chain_id, dot)
                    writer = csv.DictWriter(t_file, block_dict)
                    writer.writerow({"time": time() - self.start_time,
                                     'group_id': self.get_peer_id_by_pubkey(block.com_id),
                                     'creator': self.get_peer_id_by_pubkey(block.public_key),
                                     'type': str(block.type),
                                     'dot': block.com_dot,
                                     'transaction': str(decode_raw(block.transaction))})

        self.block_stat_file = os.path.join(os.environ['PROJECT_DIR'], 'output', 'blocks_time.csv')
        with open(self.block_stat_file, "w") as t_file:
            writer = csv.DictWriter(t_file, block_dict)
            writer.writeheader()
        self.overlay.persistence.add_observer(ChainTopic.ALL, add_block)
