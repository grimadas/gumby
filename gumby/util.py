from __future__ import annotations

from ast import literal_eval as make_tuple
from asyncio import coroutine, ensure_future, iscoroutinefunction, sleep
import os
from random import choices
from typing import Dict, List, Union


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


class Dist(object):

    def __init__(self, name: str, params: Union[List, str]) -> None:
        """Create statistical distribution using sci-py """
        self.name: str = name
        self.params: Union[List, str] = params

    def to_repr(self) -> Dict[str, Dict[str, str]]:
        return {self.__class__.__name__: {'name': self.name, 'params': str(self.params)}}

    def __str__(self) -> str:
        return self.__class__.__name__ + ": " + str(self.name) + str(self.params)

    def __repr__(self) -> str:
        return self.__class__.__name__ + ": " + str(self.name) + str(self.params)

    @classmethod
    def from_raw_str(cls, raw_dist: str) -> Dist:
        vals = raw_dist.split(',', 1)
        if len(vals) == 1:
            name = 'const'
            params = vals[0]
        else:
            name, params = vals
        params = params.strip()
        return cls(name, params)

    @classmethod
    def from_repr(cls, yaml_dict) -> Dist:
        """Create Dist object from canonical dict representation"""
        return cls(**yaml_dict)

    def generate(self, n: int = 1, seed: int = None) -> List[float]:
        """
        Generate 'n' random values with given distribution
        """
        if self.name == 'const':
            return [float(self.params)]*n
        if self.name == 'sample':
            weights = self.params['weights'] if 'weights' in self.params else None
            values = self.params['values'] if 'values' in self.params \
                else self.params
            weights = make_tuple(weights) if type(weights) == str else weights
            values = make_tuple(values) if type(values) == str else values
            return choices(values, weights=weights, k=n)

        import scipy.stats

        dist = getattr(scipy.stats, self.name)
        param = make_tuple(self.params) if type(self.params) == str else self.params
        try:
            return dist.rvs(*param[:-2], loc=param[-2], scale=param[-1], size=n)
        except TypeError:
            return dist.rvs(*param[:-1], loc=param[-1], size=n)

    def get(self, seed: int = None) -> float:
        """Get one random value from the given distribution"""
        return self.generate(1, seed)[0]
