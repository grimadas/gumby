import os
from asyncio import coroutine, ensure_future, iscoroutinefunction, sleep


def generate_keypair_trustchain():
    from ipv8.keyvault.private.libnaclkey import LibNaCLSK
    return LibNaCLSK()


def read_keypair_trustchain(keypairfilename):
    from ipv8.keyvault.private.libnaclkey import LibNaCLSK
    with open(keypairfilename, 'rb') as keyfile:
        binarykey = keyfile.read()
    return LibNaCLSK(binarykey=binarykey)


def save_keypair_trustchain(keypair, keypairfilename):
    if os.path.dirname(keypairfilename):
        os.makedirs(os.path.dirname(keypairfilename), exist_ok=True)
    with open(keypairfilename, 'wb') as keyfile:
        keyfile.write(keypair.key.sk)
        keyfile.write(keypair.key.seed)


def save_pub_key_trustchain(keypair, pubkeyfilename):
    if os.path.dirname(pubkeyfilename):
        os.makedirs(os.path.dirname(pubkeyfilename), exist_ok=True)
    with open(pubkeyfilename, 'wb') as keyfile:
        keyfile.write(keypair.key.pk)


async def interval_runner(delay, interval, task, *args):
    await sleep(delay)
    while True:
        await task(*args)
        await sleep(interval)


async def delay_runner(delay, task, *args):
    await sleep(delay)
    await task(*args)


def run_task(task, *args, delay=0, interval=0):
    if not iscoroutinefunction(task) and not callable(task):
        raise ValueError('run_task takes a (coroutine)function as a parameter')

    task = task if iscoroutinefunction(task) else coroutine(task)
    if interval:
        # The default delay for looping calls is the same as the interval
        delay = interval if delay is None else delay
        task = ensure_future(interval_runner(delay, interval, task, *args))
    elif delay:
        task = ensure_future(delay_runner(delay, task, *args))
    else:
        task = ensure_future(task(*args))
    return task
