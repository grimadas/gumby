from abc import ABCMeta
from typing import Any, Dict, Optional

from bami.backbone.block import BamiBlock
from bami.backbone.community import BamiCommunity, BlockResponse
from bami.backbone.sub_community import IPv8SubCommunityFactory, RandomWalkDiscoveryStrategy
from bami.payment.community import PaymentCommunity
from ipv8.peer import Peer

from gumby.modules.community_launcher import IPv8CommunityLauncher


class BamiPaymentCommunity(
    IPv8SubCommunityFactory,
    RandomWalkDiscoveryStrategy,
    PaymentCommunity
):
    pass


class BamiPaymentCommunityLauncher(IPv8CommunityLauncher):

    def should_launch(self, session):
        return True

    def get_overlay_class(self):
        return BamiPaymentCommunity

    def get_my_peer(self, ipv8, session):
        return Peer(session.trustchain_keypair)


class BaseDataCommunity(IPv8SubCommunityFactory, RandomWalkDiscoveryStrategy, BamiCommunity):

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
        pass

    def process_meta_block(self, block: BamiBlock) -> None:
        pass

    def join_subcommunity_gossip(self, sub_com_id: bytes) -> None:
        # 1. Data chain: exchange data blobs, process them out of order.
        self.start_gossip_sync(sub_com_id)
        self.subscribe_out_order_block(sub_com_id, self.process_data_block)

        # 2. Meta-data on the data blocks, process them in-order
        meta_prefix = self.META_PREFIX
        self.start_gossip_sync(sub_com_id, prefix=self.META_PREFIX)
        self.subscribe_in_order_block(meta_prefix + sub_com_id, self.process_meta_block)
        # Process incoming blocks in order

    def push_data_blob(self, data_blob: bytes, chain_id: bytes) -> None:
        blk = self.create_signed_block(block_type=b'data', transaction=data_blob, com_id=chain_id)
        self.share_in_community(blk, chain_id)

    def push_meta_data(self, meta_blob: bytes, chain_id: bytes):
        blk = self.create_signed_block(block_type=b'meta', transaction=meta_blob, com_id=chain_id, prefix=self.META_PREFIX)
        self.share_in_community(blk, chain_id)
