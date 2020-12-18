import json
import os
import shutil
import signal
import subprocess
from asyncio import get_event_loop

import toml

import yaml

from web3 import Web3

from gumby.experiment import experiment_callback
from gumby.modules.blockchain_module import BlockchainModule
from gumby.modules.experiment_module import static_module


@static_module
class BurrowModule(BlockchainModule):

    def __init__(self, experiment):
        super(BurrowModule, self).__init__(experiment)
        self.burrow_process = None
        self.validator_address = None
        self.contract_address = None
        self.validator_addresses = []
        self.experiment.message_callback = self

    def on_all_vars_received(self):
        super(BurrowModule, self).on_all_vars_received()
        self.transactions_manager.transfer = self.transfer

    @experiment_callback
    def transfer(self):
        # TODO only works for node 1
        yaml_json = {
            "jobs": [{
                "name": "transfer",
                "call": {
                    "destination": self.contract_address,
                    "function": "transfer",
                    "data": ["60A2A6FD47B4CC6653560132D628195B35F35A04", 1000, "AAAAA"]
                }
            }]
        }

        deploy_file_name = "transfer.yaml"
        with open(deploy_file_name, "w") as out_file:
            out_file.write(yaml.dump(yaml_json))

        process = subprocess.Popen([self.get_deploy_command(deploy_file_name)], shell=True)

    def get_deploy_command(self, script_name):
        return "/home/martijn/burrow/burrow deploy --local-abi --address %s --chain 127.0.0.1:%d --bin-path deploy_data/bin %s" % (self.validator_address, 16000 + self.experiment.my_id, script_name)

    def on_id_received(self):
        super(BurrowModule, self).on_id_received()

    def on_message(self, from_id, msg_type, msg):
        self._logger.info("Received message with type %s from peer %d", msg_type, from_id)
        if msg_type == b"validator_address":
            validator_address = msg.decode()
            self.validator_addresses.append(validator_address)

    @experiment_callback
    def generate_config(self):
        """
        Generate the initial configuration files.
        """
        self._logger.info("Generating Burrow config...")

        # Remove old config directory
        shutil.rmtree("/home/martijn/burrow_data", ignore_errors=True)

        os.mkdir("/home/martijn/burrow_data")

        cmd = "/home/martijn/burrow/burrow spec --validator-accounts=%d --full-accounts=1 > genesis-spec.json" % (self.num_validators - 1)
        process = subprocess.Popen([cmd], shell=True, cwd='/home/martijn/burrow_data')
        process.wait()

        cmd = "/home/martijn/burrow/burrow configure --genesis-spec=genesis-spec.json --pool"
        process = subprocess.Popen([cmd], shell=True, cwd='/home/martijn/burrow_data')
        process.wait()

        # RSync the configuration with other nodes
        my_host, _ = self.experiment.get_peer_ip_port_by_id(self.experiment.my_id)
        other_hosts = set()
        for peer_id in self.experiment.all_vars.keys():
            host = self.experiment.all_vars[peer_id]['host']
            if host not in other_hosts and host != my_host:
                other_hosts.add(host)
                self._logger.info("Syncing config with host %s", host)
                os.system("rsync -r --delete /home/martijn/burrow_data martijn@%s:/home/martijn/" % host)

    @experiment_callback
    def start_burrow(self):
        """
        Start Hyperledger Burrow.
        """
        if self.is_client():
            return

        config_path = os.path.join(os.getcwd(), "burrow_data")
        shutil.copytree("/home/martijn/burrow_data", config_path)

        burrow_config_file_name = "burrow00%d.toml" % (self.experiment.my_id - 1)
        burrow_config_file_path = os.path.join(config_path, burrow_config_file_name)

        with open(os.path.join(config_path, burrow_config_file_path), "r") as burrow_config_file:
            content = burrow_config_file.read()
            node_config = toml.loads(content)
            node_config["Tendermint"]["ListenPort"] = "%d" % (10000 + self.experiment.my_id)
            node_config["Tendermint"]["ListenHost"] = "0.0.0.0"
            node_config["RPC"]["Web3"]["ListenPort"] = "%d" % (12000 + self.experiment.my_id)
            node_config["RPC"]["Info"]["ListenPort"] = "%d" % (14000 + self.experiment.my_id)
            node_config["RPC"]["GRPC"]["ListenPort"] = "%d" % (16000 + self.experiment.my_id)

            self.validator_address = node_config["ValidatorAddress"]
            self._logger.info("Acting with validator address %s", self.validator_address)

            if self.experiment.my_id != 1:
                self.experiment.send_message(1, b"validator_address", self.validator_address.encode())

            # Fix the persistent peers
            persistent_peers = node_config["Tendermint"]["PersistentPeers"].split(",")
            for peer_ind in range(len(persistent_peers)):
                persistent_peer = persistent_peers[peer_ind]
                parts = persistent_peer.split(":")
                parts[-1] = "%d" % (10000 + peer_ind + 1)
                persistent_peer = ':'.join(parts)

                # Replace localhost IP
                host, _ = self.experiment.get_peer_ip_port_by_id(peer_ind + 1)
                persistent_peer = persistent_peer.replace("127.0.0.1", host)
                persistent_peers[peer_ind] = persistent_peer

            persistent_peers = ','.join(persistent_peers)
            node_config["Tendermint"]["PersistentPeers"] = persistent_peers

        with open(os.path.join(config_path, burrow_config_file_path), "w") as burrow_config_file:
            burrow_config_file.write(toml.dumps(node_config))

        cmd = "/home/martijn/burrow/burrow start --index %d --config %s > output.log 2>&1" % (self.experiment.my_id - 1, burrow_config_file_name)
        self.burrow_process = subprocess.Popen([cmd], shell=True, cwd=config_path, preexec_fn=os.setsid)

        self._logger.info("Burrow started...")

    @experiment_callback
    def deploy_contract(self):
        print("Deploying contract...")

        contracts_script_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "ethereum", "contracts")

        os.mkdir("deploy_data")
        shutil.copyfile(os.path.join(contracts_script_dir, "erc20.sol"), os.path.join("deploy_data", "erc20.sol"))

        yaml_json = {
            "jobs": [{
                "name": "deploySmartContract",
                "deploy": {
                    "contract": "erc20.sol",
                    "instance": "ERC20Basic",
                    "data": [10000000]
                }
            }]
        }

        with open(os.path.join("deploy_data", "deploy.yaml"), "w") as out_file:
            out_file.write(yaml.dump(yaml_json))

        extended_path = "/home/martijn/solc:%s" % os.getenv("PATH")  # Make sure solc can be found
        cmd = "/home/martijn/burrow/burrow deploy --address %s --chain 127.0.0.1:16001 deploy.yaml" % self.validator_address
        process = subprocess.Popen([cmd], shell=True, env={'PATH': extended_path}, cwd=os.path.join(os.getcwd(), "deploy_data"))
        process.wait()

        with open(os.path.join(os.getcwd(), "deploy_data", "deploy.output.json"), "r") as deploy_output_file:
            content = deploy_output_file.read()
            json_content = json.loads(content)

        self.contract_address = json_content["deploySmartContract"]
        self._logger.info("Smart contract address: %s" % self.contract_address)

        # TODO send ABI/deployment data to others

    @experiment_callback
    def write_stats(self):
        """
        Write away statistics.
        """
        if self.is_client():
            return

        url = 'http://localhost:%d' % (12000 + self.experiment.my_id)
        w3 = Web3(Web3.HTTPProvider(url))

        # Dump blockchain
        latest_block = w3.eth.getBlock('latest')
        with open("blockchain.txt", "w") as out_file:
            for block_nr in range(1, latest_block.number + 1):
                block = w3.eth.getBlock(block_nr)
                out_file.write(w3.toJSON(block) + "\n")

    @experiment_callback
    def stop_burrow(self):
        print("Stopping Burrow...")

        if self.burrow_process:
            os.killpg(os.getpgid(self.burrow_process.pid), signal.SIGTERM)

        loop = get_event_loop()
        loop.stop()
