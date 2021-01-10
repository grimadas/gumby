import decimal
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from asyncio import sleep, get_event_loop

import aiohttp
from aiohttp import web

import pexpect
from diem import LocalAccount, jsonrpc, stdlib, utils, testnet, diem_types, chain_ids

from ruamel.yaml import YAML

from gumby.experiment import experiment_callback
from gumby.modules.blockchain_module import BlockchainModule
from gumby.modules.experiment_module import static_module
from gumby.util import run_task


MAX_MINT = 10 ** 19  # 10 trillion libras


@static_module
class LibraModule(BlockchainModule):

    def __init__(self, experiment):
        super(LibraModule, self).__init__(experiment)
        self.libra_validator_process = None
        self.diem_client = None
        self.faucet_client = None
        self.faucet_service = None
        self.libra_path = "/home/martijn/diem"
        self.validator_config = None
        self.validator_id = None
        self.validator_ids = None
        self.peer_ids = {}
        self.waypoint_id = None
        self.validator_network_ids = {}
        self.sender_account = None
        self.receiver_account = None
        self.tx_info = {}
        self.last_tx_confirmed = -1
        self.site = None

        self.monitor_lc = None
        self.current_seq_num = 0

    def on_all_vars_received(self):
        super(LibraModule, self).on_all_vars_received()
        self.transactions_manager.transfer = self.transfer

    @experiment_callback
    def init_config(self):
        """
        Initialize the configuration. In particular, make sure the addresses of the seed nodes are correctly set.
        """
        diem_config_root_dir = os.path.join("/tmp", "diem_data_%d" % self.num_validators)

        self.validator_id = self.my_id - 1
        if self.is_client():
            return

        self._logger.info("Extracting network identifiers...")

        for validator_id in range(0, self.num_validators):
            self._logger.info("Reading initial config of validator %d...", validator_id)

            yaml = YAML()
            with open(os.path.join(diem_config_root_dir, "%d" % validator_id, "node.yaml"), "r") as node_config_file:
                node_config = yaml.load(node_config_file)

            old_validator_network_listen_address = node_config["validator_network"]["listen_address"]
            old_validator_network_listen_port = int(old_validator_network_listen_address.split("/")[-1])

            log_path = os.path.join(diem_config_root_dir, "logs", "%d.log" % validator_id)
            with open(log_path, "r") as log_file:
                for line in log_file.readlines():
                    if "Start listening for incoming connections on" in line:
                        full_network_string = line.split(" ")[10]
                        port = int(full_network_string.split("/")[4])
                        if port == old_validator_network_listen_port:
                            self._logger.info("Network ID of validator %d: %s", validator_id, full_network_string)

                            # Get the peer ID
                            peer_id = json.loads(line.split(" ")[-1])["network_context"]["peer_id"]
                            self._logger.info("Peer ID of validator %d: %s", validator_id, peer_id)
                            self.peer_ids[validator_id] = peer_id

                            # Modify the network string to insert the right IP address
                            host, _ = self.experiment.get_peer_ip_port_by_id(validator_id + 1)
                            parts = full_network_string.split("/")
                            parts[2] = host
                            full_network_string = "/".join(parts)

                            self.validator_network_ids[validator_id] = full_network_string
                            break

        # Get the waypoint ID
        with open(os.path.join(diem_config_root_dir, "waypoint")) as wp_file:
            self.waypoint_id = wp_file.read()
            self._logger.info("Waypoint ID: %s", self.waypoint_id)

        self._logger.info("Modifying configuration file...")

        yaml = YAML()
        with open(os.path.join(diem_config_root_dir, "%d" % self.validator_id, "node.yaml"), "r") as node_config_file:
            node_config = yaml.load(node_config_file)

        node_config["mempool"]["capacity_per_user"] = 10000
        node_config["consensus"]["max_block_size"] = 10000
        node_config["base"]["data_dir"] = os.getcwd()
        node_config["json_rpc"]["address"] = "0.0.0.0:%d" % (12000 + self.my_id)

        for validator_id, network_string in self.validator_network_ids.items():
            if validator_id == self.validator_id:
                continue
            node_config["validator_network"]["seed_addrs"][self.peer_ids[validator_id]] = [network_string]

        with open(os.path.join(diem_config_root_dir, "%d" % self.validator_id, "node.yaml"), "w") as crypto_config_file:
            yaml.dump(node_config, crypto_config_file)

    @experiment_callback
    def start_libra_validator(self):
        # Read the config
        if self.is_client():
            return

        self._logger.info("Starting libra validator with id %s...", self.validator_id)
        libra_exec_path = os.path.join(self.libra_path, "target", "release", "diem-node")
        diem_config_root_dir = os.path.join("/tmp", "diem_data_%d" % self.num_validators)
        config_path = os.path.join(diem_config_root_dir, "%d" % self.validator_id, "node.yaml")

        cmd = '%s -f %s > %s 2>&1' % (libra_exec_path, config_path, os.path.join(os.getcwd(), 'diem_output.log'))
        self.libra_validator_process = subprocess.Popen([cmd], shell=True, preexec_fn=os.setsid)

    async def on_mint_request(self, request):
        address = request.rel_url.query['address']
        self._logger.info("Received mint request for address %s", address)
        if re.match('^[a-f0-9]{64}$', address) is None:
            return web.Response(text="Malformed address", status=400)

        try:
            amount = decimal.Decimal(request.rel_url.query['amount'])
        except decimal.InvalidOperation:
            return web.Response(text="Bad amount", status=400)

        if amount > MAX_MINT:
            return web.Response(text="Exceeded max amount of {}".format(MAX_MINT / (10 ** 6)), status=400)

        self.faucet_client.sendline("a m {} {} XUS".format(address, amount / (10 ** 6)))
        self.faucet_client.expect("Finished sending coins from faucet!", timeout=20)

        return web.Response(text="done")

    @experiment_callback
    async def start_libra_cli(self):
        # Get the faucet host
        faucet_host, _ = self.experiment.get_peer_ip_port_by_id(1)

        if self.my_id == 1:
            # Start the minting service
            mint_key_path = os.path.join("/tmp", "diem_data_%d" % self.num_validators, "mint.key")
            cmd = "%s/target/release/cli -u http://localhost:%d -m %s --waypoint 0:%s --chain-id 4" % (self.libra_path, 12000 + self.my_id, mint_key_path, self.waypoint_id)

            self.faucet_client = pexpect.spawn(cmd)
            self.faucet_client.delaybeforesend = 0.1
            self.faucet_client.logfile = sys.stdout.buffer
            self.faucet_client.expect("Connected to validator at", timeout=3)

            # Also start the HTTP API for the faucet service
            self._logger.info("Starting faucet HTTP API...")
            app = web.Application()
            app.add_routes([web.get('/', self.on_mint_request)])

            runner = web.AppRunner(app, access_log=None)
            await runner.setup()
            # If localhost is used as hostname, it will randomly either use 127.0.0.1 or ::1
            self.site = web.TCPSite(runner, port=8000)
            await self.site.start()

        if self.is_client():
            validator_peer_id = (self.my_id - 1) % self.num_validators
            validator_host, _ = self.experiment.get_peer_ip_port_by_id(validator_peer_id + 1)
            validator_port = 12000 + validator_peer_id + 1
            self._logger.info("Spawning client that connects to validator %s (host: %s, port %s)",
                              validator_peer_id, validator_host, validator_port)
            self.diem_client = jsonrpc.Client("http://%s:%d" % (validator_host, validator_port))

    @experiment_callback
    def create_accounts(self):
        if not self.is_client():
            return

        self._logger.info("Creating accounts...")
        self.sender_account = LocalAccount.generate()
        self.receiver_account = LocalAccount.generate()

    @experiment_callback
    async def mint(self):
        if not self.is_client():
            return

        client_id = self.my_id - self.num_validators
        random_wait = 20 / self.num_clients * client_id

        await sleep(random_wait)

        faucet_host, _ = self.experiment.get_peer_ip_port_by_id(1)
        address = self.sender_account.auth_key.hex()

        async with aiohttp.ClientSession() as session:
            url = "http://" + faucet_host + ":8000/?amount=%d&address=%s" % (1000000, address)
            await session.get(url)

        print("Mint request performed!")

    @staticmethod
    def create_transaction(sender, sender_account_sequence, script, currency):
        return diem_types.RawTransaction(
            sender=sender.account_address,
            sequence_number=sender_account_sequence,
            payload=diem_types.TransactionPayload__Script(script),
            max_gas_amount=1_000_000,
            gas_unit_price=0,
            gas_currency_code=currency,
            expiration_timestamp_secs=int(time.time()) + 30,
            chain_id=chain_ids.TESTING,
        )

    @experiment_callback
    def transfer(self):
        amount = 1_000_000

        script = stdlib.encode_peer_to_peer_with_metadata_script(
            currency=utils.currency_code(testnet.TEST_CURRENCY_CODE),
            payee=self.receiver_account.account_address,
            amount=amount,
            metadata=b"",  # no requirement for metadata and metadata signature
            metadata_signature=b"",
        )
        txn = LibraModule.create_transaction(self.sender_account, self.current_seq_num, script, testnet.TEST_CURRENCY_CODE)

        signed_txn = self.sender_account.sign(txn)
        self.diem_client.submit(signed_txn)

        submit_time = int(round(time.time() * 1000))
        self.tx_info[self.current_seq_num] = (submit_time, -1)
        self.current_seq_num += 1

    @experiment_callback
    def start_monitor(self):
        if not self.is_client():
            return

        self.monitor_lc = run_task(self.monitor, interval=0.1)

    def monitor(self):
        """
        Monitor the transactions.
        """
        request_time = int(round(time.time() * 1000))

        ledger_seq_num = self.diem_client.get_account_sequence(self.sender_account.account_address)
        if ledger_seq_num == 0:
            self._logger.warning("Empty account blob!")
            return

        for seq_num in range(self.last_tx_confirmed + 1, ledger_seq_num):
            if seq_num == -1:
                continue
            self.tx_info[seq_num] = (self.tx_info[seq_num][0], request_time)

        self.last_tx_confirmed = ledger_seq_num - 1

    @experiment_callback
    def stop_monitor(self):
        if not self.is_client():
            return

        self.monitor_lc.cancel()

    @experiment_callback
    def write_stats(self):
        if not self.is_client():
            return

        # Write transaction data
        with open("transactions.txt", "w") as tx_file:
            for tx_num, tx_info in self.tx_info.items():
                tx_file.write("%d,%d,%d\n" % (tx_num, tx_info[0], tx_info[1]))

        # Write account balances
        rpc_account = self.diem_client.get_account(self.sender_account.account_address)
        balances = rpc_account.balances
        print("Sender account balances: %s", balances)

    @experiment_callback
    async def stop(self):
        print("Stopping Diem...")
        if self.libra_validator_process:
            os.killpg(os.getpgid(self.libra_validator_process.pid), signal.SIGTERM)
        if self.site:
            await self.site.stop()

        loop = get_event_loop()
        loop.stop()
