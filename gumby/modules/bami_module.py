from ipv8.peer import Peer

from gumby.modules.anydex_module import AnyDexModule
from gumby.modules.community_launcher import IPv8CommunityLauncher
from gumby.modules.experiment_module import static_module
from gumby.modules.isolated_community_loader import IsolatedIPv8CommunityLoader



@static_module
class BamiModule(AnyDexModule):
    """
    This module starts an IPv8 instance and runs AnyDex.
    """

    def create_ipv8_community_loader(self):
        # loader = super().create_ipv8_community_loader()
        loader = IsolatedIPv8CommunityLoader(self.session_id)
        print('Creating bami launcher')
        loader.set_launcher(BamiPaymentCommunityLauncher())
        print('Loader with bami added', loader)
        return loader
