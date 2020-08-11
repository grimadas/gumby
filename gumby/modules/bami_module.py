from abc import ABCMeta
from typing import Any, Dict, Optional

from bami.backbone.block import BamiBlock
from bami.backbone.community import BamiCommunity, BlockResponse
from bami.backbone.sub_community import IPv8SubCommunityFactory, RandomWalkDiscoveryStrategy
from bami.payment.community import PaymentCommunity
from ipv8.peer import Peer

from gumby.modules.community_launcher import IPv8CommunityLauncher
from gumby.util import Dist


class BamiPaymentCommunity(
    IPv8SubCommunityFactory,
    RandomWalkDiscoveryStrategy,
    PaymentCommunity
):
    pass


class BamiPaymentCommunityLauncher(IPv8CommunityLauncher):

    def should_launch(self, session):
        return session.config.get_bami_payment_enabled()

    def get_overlay_class(self):
        return BamiPaymentCommunity

    def get_my_peer(self, ipv8, session):
        return Peer(session.trustchain_keypair)


class BaseDataCommunity(IPv8SubCommunityFactory, RandomWalkDiscoveryStrategy, BamiCommunity, metaclass=ABCMeta):

    def witness_tx_well_formatted(self, witness_tx: Any) -> bool:
        pass

    def build_witness_blob(self, chain_id: bytes, seq_num: int) -> Optional[bytes]:
        pass

    def apply_witness_tx(self, block: BamiBlock, witness_tx: Any) -> None:
        pass

    def apply_confirm_tx(self, block: BamiBlock, confirm_tx: Dict) -> None:
        pass

    def apply_reject_tx(self, block: BamiBlock, reject_tx: Dict) -> None:
        pass

    def block_response(self, block: BamiBlock, wait_time: float = None, wait_blocks: int = None) -> BlockResponse:
        pass


class DataCommunity(BaseDataCommunity):
    META_PREFIX = b'meta'

    def process_data_block(self, block: BamiBlock) -> None:
        self.logger.info('Block data received %s', str(block))

    def process_meta_block(self, block: BamiBlock) -> None:
        pass

    @staticmethod
    def parse_dist(raw_dist: str) -> Dist:
        name, params = raw_dist.split(',', 1)
        params = params.strip()
        return Dist(name, params)

    def join_subcommunity_gossip(self, sub_com_id: bytes) -> None:
        # 1. Data chain: exchange data blobs, process them out of order.
        interval_dist_func = None
        if hasattr(self.settings, 'gossip_interval_dist'):
            interval_dist = self.parse_dist(self.settings.gossip_interval_dist)
            def interval_dist_func(): return interval_dist.get()

            print('Creating interval function', interval_dist_func())
        delay_dist_func = None
        if hasattr(self.settings, 'gossip_delay_dist'):
            delay_dist = self.parse_dist(self.settings.gossip_delay_dist)
            def delay_dist_func(): return delay_dist.get()

            print('Creating delay function', delay_dist_func())

        self.start_gossip_sync(sub_com_id, interval=interval_dist_func, delay=delay_dist_func)
        self.subscribe_out_order_block(sub_com_id, self.process_data_block)

        # 2. Meta-data on the data blocks, process them in-order
        meta_prefix = self.META_PREFIX
        self.start_gossip_sync(sub_com_id, prefix=self.META_PREFIX,
                               interval=interval_dist_func, delay=delay_dist_func)
        self.subscribe_in_order_block(meta_prefix + sub_com_id, self.process_meta_block)
        # Process incoming blocks in order

    def push_data_blob(self, data_blob: bytes, chain_id: bytes) -> None:
        blk = self.create_signed_block(block_type=b'data', transaction=data_blob, com_id=chain_id,
                                       use_consistent_links=False)
        self.share_in_community(blk, chain_id)

    def push_meta_data(self, meta_blob: bytes, chain_id: bytes):
        blk = self.create_signed_block(block_type=b'meta', transaction=meta_blob, com_id=chain_id,
                                       prefix=self.META_PREFIX)
        self.share_in_community(blk, chain_id)


class BamiDataCommunityLauncher(IPv8CommunityLauncher):

    def should_launch(self, session):
        return session.config.get_bami_data_enabled()

    def get_overlay_class(self):
        return DataCommunity

    def get_my_peer(self, ipv8, session):
        return Peer(session.trustchain_keypair)
