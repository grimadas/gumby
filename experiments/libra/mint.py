import argparse

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from diem import jsonrpc, testnet, stdlib, LocalAccount, utils, bcs
from diem.diem_types import Ed25519PublicKey

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Initialize a Diem network.')
    parser.add_argument('--port', metavar='n', type=int, default=4, help='The validator port')
    parser.add_argument('--mintkey', metavar='k', type=str, default=4, help='The path to the minting key')
    args = parser.parse_args()

    # Read the mining key
    with open(args.mintkey, "rb") as mint_key_file:
        content = mint_key_file.read()
        print(bcs.deserialize(content, bytes))
        mint_key = Ed25519PrivateKey.from_private_bytes(content)

    mint_account = LocalAccount(mint_key)
    print(mint_account.account_address)

    # client = jsonrpc.Client("http://127.0.0.1:%d" % args.port)
    # seq = client.get_account_sequence(testnet.DESIGNATED_DEALER_ADDRESS)
    # print(seq)
    #
    # amount = 1_000_000
    #
    # new_account = LocalAccount.generate()
    #
    # script = stdlib.encode_peer_to_peer_with_metadata_script(
    #     currency=utils.currency_code(testnet.TEST_CURRENCY_CODE),
    #     payee=new_account.account_address,
    #     amount=amount,
    #     metadata=b"",  # no requirement for metadata and metadata signature
    #     metadata_signature=b"",
    # )
    #
    # txn = diem_types.RawTransaction(
    #         sender=sender.account_address,
    #         sequence_number=sender_account_sequence,
    #         payload=diem_types.TransactionPayload__Script(script),
    #         max_gas_amount=1_000_000,
    #         gas_unit_price=0,
    #         gas_currency_code=currency,
    #         expiration_timestamp_secs=int(time.time()) + 30,
    #         chain_id=chain_ids.TESTING,
    #     )
    #
    # txn = LibraModule.create_transaction(self.sender_account, self.current_seq_num, script, testnet.TEST_CURRENCY_CODE)
    #
    # signed_txn = self.sender_account.sign(txn)
    # self.diem_client.submit(signed_txn)
