import hashlib
import json
import os
import random
import signal
import subprocess
import time
from asyncio import get_event_loop, sleep
from binascii import hexlify
from threading import Thread

import requests

from gumby.experiment import experiment_callback
from gumby.modules.blockchain_module import BlockchainModule
from gumby.modules.experiment_module import static_module


@static_module
class AvalancheModule(BlockchainModule):

    def __init__(self, experiment):
        super(AvalancheModule, self).__init__(experiment)
        self.avalanche_process = None
        self.avax_address = None
        self.avax_addresses = {}
        self.node_ids = {}
        self.transactions = {}
        self.experiment.message_callback = self

    def on_all_vars_received(self):
        super(AvalancheModule, self).on_all_vars_received()
        self.transactions_manager.transfer = self.transfer

    def on_message(self, from_id, msg_type, msg):
        self._logger.info("Received message with type %s from peer %d", msg_type, from_id)
        if msg_type == b"avax_address":
            avax_address = msg.decode()
            self.avax_addresses[from_id] = avax_address
        elif msg_type == b"node_id":
            self.node_ids[from_id] = msg.decode()

    @experiment_callback
    def sync_staking_keys(self):
        # More staking keys can be generated using the following command (on surfnet1, in /home/martijn/avalanche/staking):
        # /home/martijn/go/bin/go run gen_staker_key.go <PEER_ID_HERE>
        my_host, _ = self.experiment.get_peer_ip_port_by_id(self.experiment.my_id)
        other_hosts = set()
        for peer_id in self.experiment.all_vars.keys():
            host = self.experiment.all_vars[peer_id]['host']
            if host not in other_hosts and host != my_host:
                other_hosts.add(host)
                self._logger.info("Syncing staking keys with host %s", host)
                os.system("rsync -r --delete /home/martijn/avalanche/staking martijn@%s:/home/martijn/avalanche" % host)

    @experiment_callback
    async def start_avalanche(self):
        """
        Start an Avalanche node.
        """
        self._logger.info("This is node %d", self.my_id)

        if self.is_client():
            return

        http_port = 12000 + self.my_id
        staking_port = 14000 + self.my_id
        my_host, _ = self.experiment.get_peer_ip_port_by_id(self.my_id)
        bootstrap_host, _ = self.experiment.get_peer_ip_port_by_id(1)

        self._logger.info("Starting Avalanche...")

        if self.my_id == 1:  # Bootstrap node
            cmd = "/home/martijn/avalanche/avalanchego --public-ip=%s --snow-sample-size=2 --snow-quorum-size=2 " \
                  "--http-host= --http-port=%s --staking-port=%s --db-dir=db/node1 --staking-enabled=true " \
                  "--network-id=local --bootstrap-ips= " \
                  "--staking-tls-cert-file=/home/martijn/avalanche/staking/local/staker1.crt --plugin-dir=/home/martijn/avalanche/plugins " \
                  "--staking-tls-key-file=/home/martijn/avalanche/staking/local/staker1.key > avalanche.out" % \
                  (my_host, http_port, staking_port)
            self.avalanche_process = subprocess.Popen([cmd], shell=True, preexec_fn=os.setsid)
        else:
            cmd = "/home/martijn/avalanche/avalanchego --public-ip=%s --snow-sample-size=2 --snow-quorum-size=2 " \
                  "--http-host= --http-port=%s --staking-port=%s --db-dir=db/node%d --staking-enabled=true " \
                  "--network-id=local --bootstrap-ips=%s:14001 " \
                  "--bootstrap-ids=NodeID-7Xhw2mDxuDS44j42TCB6U5579esbSt3Lg " \
                  "--staking-tls-cert-file=/home/martijn/avalanche/staking/local/staker%d.crt --plugin-dir=/home/martijn/avalanche/plugins " \
                  "--staking-tls-key-file=/home/martijn/avalanche/staking/local/staker%d.key > avalanche.out" % \
                  (my_host, http_port, staking_port, self.my_id, bootstrap_host, self.my_id, self.my_id)

            await sleep(random.random() * 10)
            self.avalanche_process = subprocess.Popen([cmd], shell=True, preexec_fn=os.setsid)

    @experiment_callback
    def create_keystore_user(self):
        if self.is_client():
            return

        self._logger.info("Creating keystore user...")

        payload = {
            "method": "keystore.createUser",
            "params": [{
                "username": "peer%d" % self.my_id,
                "password": hexlify(hashlib.md5(b'peer%d' % self.my_id).digest()).decode()
            }],
            "jsonrpc": "2.0",
            "id": 0,
        }

        response = requests.post("http://localhost:%d/ext/keystore" % (12000 + self.my_id), json=payload).json()
        self._logger.info("Create keystore response: %s", response)

    @experiment_callback
    def import_funds(self):
        if self.is_client():
            return

        self._logger.info("Importing initial funds...")

        payload = {
            "method": "avm.importKey",
            "params": [{
                "username": "peer%d" % self.my_id,
                "password": hexlify(hashlib.md5(b'peer%d' % self.my_id).digest()).decode(),
                "privateKey": "PrivateKey-ewoqjP7PxY4yr3iLTpLisriqt94hdyDFNgchSxGGztUrTXtNN"
            }],
            "jsonrpc": "2.0",
            "id": 0,
        }

        response = requests.post("http://localhost:%d/ext/bc/X" % (12000 + self.my_id), json=payload).json()
        self._logger.info("Import funds response: %s", response)
        self.avax_address = response["result"]["address"]

        for client_index in range(self.num_validators + 1, self.num_validators + self.num_clients + 1):
            self.experiment.send_message(client_index, b"avax_address", self.avax_address.encode())

    @experiment_callback
    def create_address(self):
        if self.is_client() or self.my_id == 1:  # The first node uses the initial address
            return

        self._logger.info("Creating address...")

        payload = {
            "method": "avm.createAddress",
            "params": [{
                "username": "peer%d" % self.my_id,
                "password": hexlify(hashlib.md5(b'peer%d' % self.my_id).digest()).decode(),
            }],
            "jsonrpc": "2.0",
            "id": 0,
        }

        response = requests.post("http://localhost:%d/ext/bc/X" % (12000 + self.my_id), json=payload).json()
        self._logger.info("Create address response: %s", response)
        self.avax_address = response["result"]["address"]

        # Send the address to the first node
        self.experiment.send_message(1, b"avax_address", self.avax_address.encode())

        # Send the address to the clients
        for client_index in range(self.num_validators + 1, self.num_validators + self.num_clients + 1):
            self.experiment.send_message(client_index, b"avax_address", self.avax_address.encode())

    @experiment_callback
    def share_node_id(self):
        if self.is_client() or self.my_id <= 5:
            return

        self._logger.info("Sharing node ID with bootstrap peer")

        # Get the node ID
        payload = {
            "method": "info.getNodeID",
            "params": [{}],
            "jsonrpc": "2.0",
            "id": 0,
        }

        response = requests.post("http://localhost:%d/ext/info" % (12000 + self.my_id), json=payload).json()
        node_id = response["result"]["nodeID"]
        self.experiment.send_message(1, b"node_id", node_id.encode())

    @experiment_callback
    async def transfer_funds_to_others(self):
        if self.is_client():
            return

        self._logger.info("Transferring funds to others...")

        for avax_address in self.avax_addresses.values():
            self._logger.info("Transferring initial funds to address %s", avax_address)
            payload = {
                "method": "wallet.send",
                "params": [{
                    "assetID": "AVAX",
                    "amount": 1000000000,
                    "to": avax_address,
                    "username": "peer%d" % self.my_id,
                    "password": hexlify(hashlib.md5(b'peer%d' % self.my_id).digest()).decode(),
                }],
                "jsonrpc": "2.0",
                "id": 0,
            }

            response = requests.post("http://localhost:%d/ext/bc/X/wallet" % (12000 + self.my_id), json=payload).json()
            self._logger.info("Transfer funds response: %s", response)

    @experiment_callback
    async def register_validators(self):
        if self.is_client():
            return

        self._logger.info("Registering validators...")

        # Import the bootstrap address in the P chain
        payload = {
            "method": "platform.importKey",
            "params": [{
                "username": "peer%d" % self.my_id,
                "password": hexlify(hashlib.md5(b'peer%d' % self.my_id).digest()).decode(),
                "privateKey": "PrivateKey-ewoqjP7PxY4yr3iLTpLisriqt94hdyDFNgchSxGGztUrTXtNN"
            }],
            "jsonrpc": "2.0",
            "id": 0,
        }

        response = requests.post("http://localhost:%d/ext/P" % (12000 + self.my_id), json=payload).json()
        self._logger.info("Import funds response: %s", response)
        staking_address = response["result"]["address"]

        tx_ids = []

        for validator_id, node_id in self.node_ids.items():
            # Create a reward address
            payload = {
                "method": "platform.createAddress",
                "params": [{
                    "username": "peer%d" % self.my_id,
                    "password": hexlify(hashlib.md5(b'peer%d' % self.my_id).digest()).decode(),
                }],
                "jsonrpc": "2.0",
                "id": 0,
            }

            response = requests.post("http://localhost:%d/ext/bc/P" % (12000 + self.my_id), json=payload).json()
            self._logger.info("Create address response: %s", response)
            reward_address = response["result"]["address"]

            # Register as validator
            payload = {
                "method": "platform.addValidator",
                "params": [{
                    "nodeID": node_id,
                    "from": [staking_address],
                    "startTime": '%d' % (int(time.time()) + 15),
                    "endTime": '%d' % (int(time.time()) + 30 * 24 * 3600),
                    "stakeAmount": 2000000000000,
                    "rewardAddress": reward_address,
                    "delegationFeeRate": 10,
                    "username": "peer%d" % self.my_id,
                    "password": hexlify(hashlib.md5(b'peer%d' % self.my_id).digest()).decode(),
                }],
                "jsonrpc": "2.0",
                "id": 0,
            }

            response = requests.post("http://localhost:%d/ext/P" % (12000 + self.my_id), json=payload).json()
            self._logger.info("Add validator response: %s", response)
            tx_id = response["result"]["txID"]
            tx_ids.append(tx_id)

            await sleep(0.15)

        await sleep(15)

        for tx_id in tx_ids:
            self._logger.info("Getting status of validator tx")
            payload = {
                "method": "platform.getTxStatus",
                "params": [{
                    "txID": tx_id,
                    "includeReason": True,
                }],
                "jsonrpc": "2.0",
                "id": 0,
            }

            response = requests.post("http://localhost:%d/ext/P" % (12000 + self.my_id), json=payload).json()
            self._logger.info("Validator tx status: %s", response)

    @experiment_callback
    def transfer(self):
        if not self.is_client():
            return

        validator_peer_id = ((self.my_id - 1) % self.num_validators) + 1
        validator_host, _ = self.experiment.get_peer_ip_port_by_id(validator_peer_id)

        def create_and_submit_tx():
            submit_time = int(round(time.time() * 1000))

            payload = {
                "method": "wallet.send",
                "params": [{
                    "assetID": "AVAX",
                    "amount": 100,
                    "to": self.avax_addresses[validator_peer_id],
                    "username": "peer%d" % validator_peer_id,
                    "password": hexlify(hashlib.md5(b'peer%d' % validator_peer_id).digest()).decode(),
                }],
                "jsonrpc": "2.0",
                "id": 0,
            }

            response = requests.post("http://%s:%d/ext/bc/X/wallet" % (validator_host, 12000 + validator_peer_id), json=payload).json()
            self._logger.info("Transfer funds response: %s", response)
            tx_id = response["result"]["txID"]
            self.transactions[tx_id] = (submit_time, -1)

            # Poll the status of this transaction
            for _ in range(20):
                payload = {
                    "method": "avm.getTxStatus",
                    "params": [{
                        "txID": tx_id,
                    }],
                    "jsonrpc": "2.0",
                    "id": 0,
                }

                response = requests.post("http://%s:%d/ext/bc/X" % (validator_host, 12000 + validator_peer_id), json=payload).json()
                if response["result"]["status"] == "Accepted":
                    confirm_time = int(round(time.time() * 1000))
                    self.transactions[tx_id] = (self.transactions[tx_id][0], confirm_time)
                    break

                time.sleep(0.5)

        t = Thread(target=create_and_submit_tx)
        t.daemon = True
        t.start()

    @experiment_callback
    def write_stats(self):
        if self.is_client():
            # Write transactions
            with open("transactions.txt", "w") as tx_file:
                for tx_id, tx_info in self.transactions.items():
                    tx_file.write("%s,%d,%d\n" % (tx_id, tx_info[0], tx_info[1]))

            return

        # Write the balance
        payload = {
            "method": "avm.getBalance",
            "params": [{
                "address": self.avax_address,
                "assetID": "AVAX"
            }],
            "jsonrpc": "2.0",
            "id": 0,
        }

        response = requests.post("http://localhost:%d/ext/bc/X" % (12000 + self.my_id), json=payload).json()
        self._logger.info("Request balance response: %s" % response)
        with open("balance.txt", "w") as balance_file:
            balance_file.write(response["result"]["balance"])

        # Write the current validators
        payload = {
            "method": "platform.getCurrentValidators",
            "params": [{}],
            "jsonrpc": "2.0",
            "id": 0,
        }

        response = requests.post("http://localhost:%d/ext/P" % (12000 + self.my_id), json=payload).json()
        with open("validators.txt", "w") as validators_file:
            validators_file.write(json.dumps(response["result"]))

    @experiment_callback
    def stop(self):
        if self.avalanche_process:
            self._logger.info("Stopping Avalanche...")
            os.killpg(os.getpgid(self.avalanche_process.pid), signal.SIGTERM)

        loop = get_event_loop()
        loop.stop()
