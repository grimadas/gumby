from ipv8.peer import Peer

from gumby.modules.community_launcher import IPv8CommunityLauncher
from gumby.modules.experiment_module import static_module
from gumby.modules.isolated_community_loader import IsolatedIPv8CommunityLoader
from gumby.modules.tribler_module import TriblerModule


class BamiPaymentCommunityLauncher(IPv8CommunityLauncher):

    def should_launch(self, session):
        return True

    def get_overlay_class(self):
        from bami.payment.community import PaymentCommunity
        return PaymentCommunity

    def get_my_peer(self, ipv8, session):
        return Peer(session.trustchain_keypair)


@static_module
class BamiModule(TriblerModule):
    """
    This module starts an IPv8 instance and runs AnyDex.
    """

    def create_ipv8_community_loader(self):
        print('Creating bami launcher')
        loader = super().create_ipv8_community_loader()
        loader.set_launcher(BamiPaymentCommunityLauncher())
        print('Loader with bami added', loader)
        return loader
