from asyncio import Future
from binascii import hexlify
import json
from os import environ, getpid, makedirs, path, symlink
import time

from ipv8_service import IPv8

from gumby.anydex_config import AnyDexConfig
from gumby.experiment import experiment_callback
from gumby.modules.bami_module import BamiPaymentCommunityLauncher, BamiDataCommunityLauncher
from gumby.modules.community_launcher import DHTCommunityLauncher, MarketCommunityLauncher, TrustChainCommunityLauncher
from gumby.modules.experiment_module import ExperimentModule, static_module
from gumby.modules.isolated_community_loader import IsolatedIPv8CommunityLoader
from gumby.util import read_keypair_trustchain, run_task


class GumbyMinimalLm(object):
    """
    Minimal lm object, used to remain compatibility with Tribler-entangled code.
    """
    pass


class GumbyMinimalSession(object):
    """
    Minimal Gumby session, with a configuration.
    """

    def __init__(self, config):
        self.config = config
        self.lm = GumbyMinimalLm()
        self.trustchain_keypair = None


@static_module
class AnyDexModule(ExperimentModule):
    """
    This module starts an IPv8 instance and runs AnyDex.
    """

    def __init__(self, experiment):
        super(AnyDexModule, self).__init__(experiment)
        self.has_ipv8 = True
        self.session = None
        self.ipv8_port = None
        self.tribler_config = None
        self.session_id = environ['SYNC_HOST'] + environ['SYNC_PORT']
        self._logger.info('Anydex module created')
        self.custom_ipv8_community_loader = self.create_ipv8_community_loader()
        self.ipv8_available = Future()
        self.ipv8 = None

    def write_ipv8_statistics(self):
        statistics = self.ipv8.endpoint.statistics

        # Cleanup this dictionary
        time_elapsed = time.time() - self.experiment.scenario_runner.exp_start_time
        new_dict = {"time": time_elapsed, "stats": {}}
        for overlay_prefix, messages_dict in statistics.items():
            hex_prefix = hexlify(overlay_prefix).decode('utf-8')
            new_dict["stats"][hex_prefix] = {}
            for msg_id, msg_stats in messages_dict.items():
                new_dict["stats"][hex_prefix][msg_id] = msg_stats.to_dict()

        with open('ipv8_statistics.txt', 'a') as statistics_file:
            statistics_file.write(json.dumps(new_dict) + '\n')

    def on_id_received(self):
        self._logger.info('Id received on anydex')
        self.tribler_config = self.setup_config()
        super(AnyDexModule, self).on_id_received()

    def create_ipv8_community_loader(self):
        self._logger.info('Creating community loader')
        loader = IsolatedIPv8CommunityLoader(self.session_id)
        loader.set_launcher(TrustChainCommunityLauncher())
        loader.set_launcher(MarketCommunityLauncher())
        loader.set_launcher(DHTCommunityLauncher())
        loader.set_launcher(BamiPaymentCommunityLauncher())
        loader.set_launcher(BamiDataCommunityLauncher())
        return loader

    @experiment_callback
    def enable_bami_payments(self):
        self.tribler_config.set_bami_payment_enabled(True)

    @experiment_callback
    def enable_bami_data(self):
        self.tribler_config.set_bami_data_enabled(True)

    @experiment_callback
    async def start_session(self):
        from ipv8.configuration import get_default_configuration
        self._logger.info('Starting session')

        ipv8_config = get_default_configuration()
        ipv8_config['port'] = self.tribler_config.get_ipv8_port()
        ipv8_config['overlays'] = []
        ipv8_config['keys'] = []  # We load the keys ourselves
        self.ipv8 = IPv8(ipv8_config, enable_statistics=self.tribler_config.get_ipv8_statistics())
        self.session = GumbyMinimalSession(self.tribler_config)
        self.session.trustchain_keypair = read_keypair_trustchain(self.tribler_config.get_trustchain_keypair_filename())
        # Load overlays
        self.custom_ipv8_community_loader.load(self.ipv8, self.session)
        await self.ipv8.start()
        self.ipv8_available.set_result(self.ipv8)

    @experiment_callback
    def enable_ipv8_statistics(self):
        self.tribler_config.set_ipv8_statistics(True)

    @experiment_callback
    def start_ipv8_statistics_monitor(self):
        run_task(self.write_ipv8_statistics, interval=1)

    @experiment_callback
    def set_ipv8_port(self, port):
        self.ipv8_port = int(port)

    @experiment_callback
    def isolate_ipv8_overlay(self, name):
        self.custom_ipv8_community_loader.isolate(name)

    @experiment_callback
    async def stop_session(self):
        await self.ipv8.stop()

    @experiment_callback
    def write_overlay_statistics(self):
        """
        Write information about the IPv8 overlay networks to a file.
        """
        with open('overlays.txt', 'w') as overlays_file:
            overlays_file.write("name,pub_key,peers\n")
            for overlay in self.session.ipv8.overlays:
                overlays_file.write("%s,%s,%d\n" % (overlay.__class__.__name__,
                                                    hexlify(overlay.my_peer.public_key.key_to_bin()),
                                                    len(overlay.get_peers())))

        # Write verified peers
        with open('verified_peers.txt', 'w') as peers_file:
            for peer in self.session.ipv8.network.verified_peers:
                peers_file.write('%d\n' % (peer.address[1] - 12000))

        # Write bandwidth statistics
        with open('bandwidth.txt', 'w') as bandwidth_file:
            bandwidth_file.write("%d,%d" % (self.session.ipv8.endpoint.bytes_up,
                                            self.session.ipv8.endpoint.bytes_down))

    def setup_config(self):
        if self.ipv8_port is None:
            self.ipv8_port = 12000 + self.experiment.my_id
        self._logger.info("IPv8 port set to %d", self.ipv8_port)

        my_state_path = path.abspath(path.join(environ["OUTPUT_DIR"], ".%s-%d-%d" % (self.__class__.__name__,
                                                                                     getpid(), self.my_id)))
        makedirs(my_state_path)
        bootstrap_file = path.join(environ['OUTPUT_DIR'], 'bootstraptribler.txt')
        if path.exists(bootstrap_file):
            symlink(bootstrap_file, path.join(my_state_path, 'bootstraptribler.txt'))
        else:
            base_tracker_port = int(environ['TRACKER_PORT'])
            port_range = range(base_tracker_port, base_tracker_port + 4)
            with open(path.join(my_state_path, 'bootstraptribler.txt'), "w+") as f:
                f.write("\n".join(["%s %d" % (environ['HEAD_HOST'], port) for port in port_range]))
            bootstrap_file = path.join(my_state_path, 'bootstraptribler.txt')

        # We manually update the IPv8 bootstrap servers since IPv8 does not use the bootstraptribler.txt file.
        from ipv8 import community
        community._DEFAULT_ADDRESSES = []
        community._DNS_ADDRESSES = []
        with open(bootstrap_file, 'r') as bfile:
            for line in bfile.readlines():
                parts = line.split(" ")
                if not parts:
                    continue
                community._DNS_ADDRESSES.append((parts[0], int(parts[1])))

        config = AnyDexConfig()
        self._logger.info('Writing new keypairname: tc_keypair_' + str(self.experiment.my_id))
        config.set_trustchain_keypair_filename("tc_keypair_" + str(self.experiment.my_id))
        config.set_state_dir(my_state_path)
        config.set_ipv8_port(self.ipv8_port)
        return config
