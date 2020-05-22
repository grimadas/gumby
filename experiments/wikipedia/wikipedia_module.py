import os

from gumby.experiment import experiment_callback
from gumby.modules.community_experiment_module import IPv8OverlayExperimentModule
from gumby.modules.experiment_module import static_module
from ipv8.attestation.backbone.community import PlexusCommunity
from ipv8.attestation.backbone.datastore.consistency import ChainState
from ipv8.attestation.backbone.datastore.utils import key_to_id


class MockChainState(ChainState):

    def __init__(self, name):
        super().__init__(name)

    def init_state(self):
        """
        Initialize state when there no blocks
        @return: Fresh new state
        """
        return {'total': 0, 'vals': [0, 0], 'front': list(), 'stakes': dict()}

    def apply_block(self, prev_state, block):
        """
        Apply block(with delta) to the prev_state
        @param prev_state:
        @param block:
        @return: Return new_state
        """
        # get from  front last value
        if block.type == b'edit':
            delta = block.transaction['size'] - prev_state['vals'][0]
            sh_hash = key_to_id(block.hash)
            peer = key_to_id(block.public_key)
            total = prev_state['total'] + abs(delta)
            new_stakes = dict()
            new_stakes.update(prev_state['stakes'])
            if peer not in prev_state['stakes']:
                new_stakes[peer] = abs(delta)
            else:
                new_stakes[peer] += abs(delta)

            return {'total': total,
                    'front': [sh_hash],
                    'vals': [block.transaction['id'], delta, peer],
                    'stakes': new_stakes
                    }

    def merge(self, old_state, new_state):
        """
        Merge two potentially conflicting states
        @param old_state:
        @param new_state:
        @return: Fresh new state of merged states
        """
        if not old_state:
            # There are no conflicts
            return new_state

        # Check if there are actually conflicting by verifying the fronts
        merged_state = dict()
        if not set(new_state['front']).issubset(set(old_state['front'])):
            # merge fronts
            merged_state['front'] = sorted(list(set(old_state['front']) | set(new_state['front'])))
            merged_state['total'] = old_state['total'] + abs(new_state['vals'][1])
            merged_state['vals'] = [old_state['vals'][0] + new_state['vals'][1],
                                    old_state['vals'][1] + new_state['vals'][1]]
            p = new_state['vals'][2]
            delta = new_state['vals'][1]
            merged_state['stakes'] = dict()
            merged_state['stakes'].update(old_state['stakes'])
            if p not in merged_state['stakes']:
                merged_state['stakes'][p] = abs(delta)
            else:
                merged_state['stakes'][p] += abs(delta)
            merged_state['stakes'] = sorted(merged_state['stakes'].items())

            return merged_state
        else:
            return old_state


@static_module
class PlexusModule(IPv8OverlayExperimentModule):
    def __init__(self, experiment):
        super().__init__(experiment, PlexusCommunity)

    @experiment_callback
    def sub_communities(self, coms):
        new_coms = [str.encode(k) for k in coms.split(',')]
        self.overlay.subscribe_to_multi_community(new_coms)
        self._logger.info("Subing to communities an edit %s", new_coms)
        for com_id in new_coms:
            self._logger.info("Adding chain state ")
            self.overlay.persistence.add_chain_state(com_id, MockChainState('sum'))

    @experiment_callback
    def edit(self, page_id, size, rev_id):
        comm_id = str.encode(page_id)
        transaction = {"rev_id": rev_id, "size": size}
        self._logger.info("Creating an edit %s", transaction)
        self.overlay.self_sign_block(block_type=b'edit', transaction=transaction, com_id=comm_id)

    @experiment_callback
    def revert(self, page_id, reverted, rever_to):
        comm_id = str.encode(page_id)
        transaction = {"revert": reverted, "revert_to": rever_to}
        self._logger.info("Creating an revert %s", transaction)
        self.overlay.self_sign_block(block_type=b'revert', transaction=transaction, com_id=comm_id)
