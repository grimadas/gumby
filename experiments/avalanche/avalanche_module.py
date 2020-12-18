import os
import signal
import subprocess
from asyncio import get_event_loop

from gumby.experiment import experiment_callback
from gumby.modules.blockchain_module import BlockchainModule
from gumby.modules.experiment_module import static_module


@static_module
class AvalancheModule(BlockchainModule):

    def __init__(self, experiment):
        super(AvalancheModule, self).__init__(experiment)
        self.avalanche_process = None

    def on_all_vars_received(self):
        super(AvalancheModule, self).on_all_vars_received()
        self.transactions_manager.transfer = self.transfer

    @experiment_callback
    async def start_avalanche(self):
        """
        Start an Avalanche node.
        """
        if self.is_client():
            return

        http_port = 12000 + self.my_id
        staking_port = 14000 + self.my_id

        if self.my_id == 1:  # Bootstrap node  # TODO this is all local!
            cmd = "/home/martijn/avalanche/avalanchego --public-ip=127.0.0.1 --snow-sample-size=2 --snow-quorum-size=2 " \
                  "--http-port=%s --staking-port=%s --db-dir=db/node1 --staking-enabled=true " \
                  "--network-id=local --bootstrap-ips= " \
                  "--staking-tls-cert-file=/home/martijn/avalanche/staking/local/staker1.crt " \
                  "--staking-tls-key-file=/home/martijn/avalanche/staking/local/staker1.key > avalanche.out" % (http_port, staking_port)
        else:
            cmd = "/home/martijn/avalanche/avalanchego --public-ip=127.0.0.1 --snow-sample-size=2 --snow-quorum-size=2 " \
                  "--http-port=%s --staking-port=%s --db-dir=db/node%d --staking-enabled=true " \
                  "--network-id=local --bootstrap-ips=127.0.0.1:14001 " \
                  "--bootstrap-ids=NodeID-7Xhw2mDxuDS44j42TCB6U5579esbSt3Lg " \
                  "--staking-tls-cert-file=/home/martijn/staking/local/staker%d.crt " \
                  "--staking-tls-key-file=/home/martijn/staking/local/staker%d.key > avalanche.out" % \
                  (http_port, staking_port, self.my_id, self.my_id, self.my_id)

        self.avalanche_process = subprocess.Popen([cmd], shell=True, preexec_fn=os.setsid)

    @experiment_callback
    def transfer(self):
        if not self.is_client():
            return

        # TODO implement

    @experiment_callback
    def stop(self):
        if self.avalanche_process:
            self._logger.info("Stopping Avalanche...")
            os.killpg(os.getpgid(self.avalanche_process.pid), signal.SIGTERM)

        loop = get_event_loop()
        loop.stop()
