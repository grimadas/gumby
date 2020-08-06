from base64 import b64decode, b64encode
import csv
from decimal import Decimal
import os
from time import time
from typing import Any, Optional, Type

from bami.backbone.block import BamiBlock
from bami.backbone.community import BamiCommunity
from bami.backbone.utils import decode_raw, encode_raw
from bami.payment.exceptions import InsufficientBalanceException

from gumby.experiment import experiment_callback
from gumby.modules.bami_module import BamiPaymentCommunity, DataCommunity
from gumby.modules.community_experiment_module import IPv8OverlayExperimentModule
from gumby.modules.experiment_module import static_module
from gumby.util import run_task


class BaseBamiExperiments(IPv8OverlayExperimentModule):
    def __init__(self, experiment: Any, community_class: Type[BamiCommunity] = None):
        super().__init__(experiment, community_class)
        self._logger.info('Creating experiment session: bami with class %s', community_class.__name__)

        self.block_stat_file = None
        self.start_time = None

    def change_settings_from_environ(self):
        if os.environ.get('WITNESS_BLOCK_DELTA'):
            self.overlay.settings.witness_block_delta = int(float(os.environ.get('WITNESS_BLOCK_DELTA')))
        if os.environ.get('WITNESS_DELTA_TIME'):
            self.overlay.settings.witness_delta_time = float(float(os.environ.get('WITNESS_DELTA_TIME')))
        if os.environ.get('PUSH_GOSSIP_FANOUT'):
            self.overlay.settings.push_gossip_fanout = int(os.environ.get('PUSH_GOSSIP_FANOUT'))
        if os.environ.get('PUSH_GOSSIP_TTL'):
            self.overlay.settings.push_gossip_ttl = int(os.environ.get('PUSH_GOSSIP_TTL'))
        if os.environ.get('GOSSIP_MAX_DELAY'):
            self.overlay.settings.gossip_sync_max_delay = float(os.environ.get('GOSSIP_MAX_DELAY'))
        if os.environ.get('GOSSIP_DELTA_TIME'):
            self.overlay.settings.gossip_sync_time = float(os.environ.get('GOSSIP_DELTA_TIME'))
        if os.environ.get('GOSSIP_COLLECT_TIME'):
            self.overlay.settings.gossip_collect_time = float(os.environ.get('GOSSIP_COLLECT_TIME'))
        if os.environ.get('BLOCK_SIGN_DELTA_TIME'):
            self.overlay.settings.block_sign_delta = float(os.environ.get('BLOCK_SIGN_DELTA_TIME'))
        if os.environ.get('BLOCK_SIGN_DELTA_TIME'):
            self.overlay.settings.block_sign_delta = float(os.environ.get('BLOCK_SIGN_DELTA_TIME'))
        if os.environ.get('BLOCK_MAX_WAIT_TIME'):
            self.overlay.settings.max_wait_time = float(os.environ.get('BLOCK_MAX_WAIT_TIME'))
        if os.environ.get('PULL_GOSSIP_FANOUT'):
            self.overlay.settings.gossip_fanout = int(os.environ.get('PULL_GOSSIP_FANOUT'))
        if os.environ.get('DIVERSITY_CONFIRM'):
            self.overlay.settings.diversity_confirm = int(os.environ.get('DIVERSITY_CONFIRM'))

    def on_ipv8_available(self, _):
        # Disable threadpool messages
        self.overlay._use_main_thread = True
        self.overlay.ipv8 = self.ipv8
        self.change_settings_from_environ()

    def get_peer_id_by_pubkey(self, pub_key: bytes) -> Optional[str]:
        pub_key_convert = b64encode(pub_key).decode('utf-8')
        for p_id in self.all_vars:
            if pub_key_convert == self.all_vars[p_id]['public_key']:
                return p_id
        else:
            return None

    def join_subcommunity_by_peer_id(self, peer_id: str) -> None:
        """Peer creates a sub-community with own id as the manager peer"""
        group_id = b64decode(self.get_peer_public_key(peer_id))
        self._logger.info('Joining sub-community with id %s', group_id)
        self.overlay.subscribe_to_subcom(group_id)

    def add_block(self, block: BamiBlock) -> None:
        block_dict = ['time', 'group_id', 'creator', 'type', 'dot', 'transaction']
        self._logger.info('Adding block on overlay: %s, communtiy_class: %s', self.overlay, self.community_class)
        if self.overlay:
            with open(self.block_stat_file, "a") as t_file:
                writer = csv.DictWriter(t_file, block_dict)
                writer.writerow({"time": time() - self.start_time,
                                 'group_id': self.get_peer_id_by_pubkey(block.com_id),
                                 'creator': self.get_peer_id_by_pubkey(block.public_key),
                                 'type': str(block.type),
                                 'dot': block.com_dot,
                                 'transaction': str(decode_raw(block.transaction))})

    def init_block_stat_file(self):
        # Open projects output directory and save blocks arrival time
        block_dict = ['time', 'group_id', 'creator', 'type', 'dot', 'transaction']
        self.block_stat_file = os.path.join(os.environ['PROJECT_DIR'], 'output',
                                            'blocks_time_' + str(self.my_id) + '.csv')
        self._logger.info('Creating block state file %s', self.block_stat_file)
        with open(self.block_stat_file, "w") as t_file:
            writer = csv.DictWriter(t_file, block_dict)
            writer.writeheader()
        self.start_time = time()


@static_module
class BamiDataExperiments(BaseBamiExperiments):

    def __init__(self, experiment: Any) -> None:
        super().__init__(experiment, DataCommunity)

        self.blob_creation_tasks = {}
        self.meta_creation_tasks = {}

        self.META_PREFIX = b'meta'

    def on_id_received(self):
        super().on_id_received()
        self.init_block_stat_file()

    @experiment_callback
    def join_group(self, peer_id: str = '1') -> None:
        self.join_subcommunity_by_peer_id(peer_id)

        com_id = b64decode(self.get_peer_public_key(peer_id))
        self.overlay.subscribe_out_order_block(com_id, self.add_block)

        # 2. Meta-data on the data blocks, process them in-order
        meta_prefix = self.META_PREFIX
        self.overlay.subscribe_in_order_block(meta_prefix + com_id, self.add_block)

    @experiment_callback
    def create_random_blob(self, peer_id: str, blob_size: int) -> None:
        blob = encode_raw(b'0' * int(blob_size))
        com_id = b64decode(self.get_peer_public_key(peer_id))
        self.overlay.push_data_blob(blob, com_id)

    @experiment_callback
    def start_creating_blobs(self, interval: float = 1, peer_id: str = '1', blob_size: int = 300) -> None:
        if os.environ.get('NUM_PRODUCERS'):
            num_producers = int(os.environ.get('NUM_PRODUCERS'))
        else:
            num_producers = -1
        if os.environ.get('BLOCK_INTERVAL'):
            interval = float(os.environ.get('BLOCK_INTERVAL'))
        if num_producers < 0 or self.my_id <= num_producers:
            self.blob_creation_tasks[peer_id] = run_task(self.create_random_blob, peer_id, blob_size,
                                                         interval=float(interval))

    @experiment_callback
    def stop_creating_blobs(self):
        for task in self.blob_creation_tasks.values():
            task.cancel()

    @experiment_callback
    def create_random_meta_block(self, peer_id: str) -> None:
        meta_blob = encode_raw({b'value': b'val2', b'value1': b'val3'})
        com_id = b64decode(self.get_peer_public_key(peer_id))
        self.overlay.push_meta_data(meta_blob, com_id)

    @experiment_callback
    def start_creating_meta_blocks(self, interval: float = 1, peer_id: str = '1') -> None:
        if os.environ.get('NUM_PRODUCERS'):
            num_producers = int(os.environ.get('NUM_PRODUCERS'))
        else:
            num_producers = -1

        if os.environ.get('BLOCK_INTERVAL'):
            interval = float(os.environ.get('BLOCK_INTERVAL'))

        if num_producers < 0 or self.my_id <= num_producers:
            self.blob_creation_tasks['meta' + peer_id] = run_task(self.create_random_meta_block, peer_id,
                                                                  interval=float(interval))

    @experiment_callback
    def stop_creating_meta_blocks(self):
        for task in self.blob_creation_tasks.values():
            task.cancel()


@static_module
class BamiPaymentExperiments(BaseBamiExperiments):
    def __init__(self, experiment):
        super().__init__(experiment, BamiPaymentCommunity)

    def on_id_received(self):
        super().on_id_received()
        self.init_block_stat_file()

    @experiment_callback
    def join_group(self, peer_id: str) -> None:
        self.join_subcommunity_by_peer_id(peer_id)

        com_id = b64decode(self.get_peer_public_key(peer_id))
        self.overlay.subscribe_in_order_block(com_id, self.add_block)

    @experiment_callback
    def mint(self, val: str = None) -> None:
        context = self.overlay.context
        # Change to default
        if not val:
            value = Decimal(99, context)
        else:
            value = Decimal(val, context)
        self.overlay.mint(value=value)
        print("Mint request performed!")

    @experiment_callback
    def transfer(self, group_experiment_id: str, counter_party_id: str, value: str) -> None:
        context = self.overlay.context
        group_id = b64decode(self.get_peer_public_key(group_experiment_id))
        counter_party_key_id = b64decode(self.get_peer_public_key(counter_party_id))
        value = Decimal(value, context)
        try:
            self.overlay.spend(group_id, counter_party_key_id, value=value)
        except InsufficientBalanceException as e:
            print("Balance is not sufficient ", e)

    def random_transfer(self, group_id: str, value: str) -> None:
        from random import choice
        counter_peer = choice(list(set(self.all_vars.keys()) - {self.my_id}))
        self.transfer(group_id, counter_peer, value)

    @experiment_callback
    def start_transfering_randomly(self, interval: float = 1, peer_id: str = '1', transfer_amount: str = '1') -> None:
        if os.environ.get('NUM_PRODUCERS'):
            num_producers = int(os.environ.get('NUM_PRODUCERS'))
        else:
            num_producers = -1

        if os.environ.get('BLOCK_INTERVAL'):
            interval = float(os.environ.get('BLOCK_INTERVAL'))

        if num_producers < 0 or self.my_id <= num_producers:
            # Choose counter-party peer:
            self.blob_creation_tasks[peer_id] = run_task(self.random_transfer, peer_id, transfer_amount,
                                                         interval=float(interval))

    @experiment_callback
    def stop_creating_blocks(self):
        for task in self.blob_creation_tasks.values():
            task.cancel()
