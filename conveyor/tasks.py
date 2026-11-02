from __future__ import annotations

import hashlib
import random
import time
from collections import Counter
from typing import Callable

REGISTRY: dict[str, Callable[..., object]] = {}


def task(name: str | None = None):
    def register(fn):
        REGISTRY[name or fn.__name__] = fn
        return fn

    return register


@task()
def count_primes(limit: int) -> int:
    """CPU-bound: sieve of Eratosthenes."""
    sieve = bytearray([1]) * (limit + 1)
    sieve[:2] = b"\x00\x00"
    for i in range(2, int(limit ** 0.5) + 1):
        if sieve[i]:
            sieve[i * i::i] = bytearray(len(range(i * i, limit + 1, i)))
    return sum(sieve)


@task()
def word_count(text: str, top: int = 5) -> list:
    return Counter(w.strip(".,;:!?").lower() for w in text.split()).most_common(top)


@task()
def hash_chain(seed: str, rounds: int = 100_000) -> str:
    digest = seed.encode()
    for _ in range(rounds):
        digest = hashlib.sha256(digest).digest()
    return digest.hex()[:16]


@task()
def flaky(fail_rate: float = 0.5, work: float = 0.05) -> str:
    """Simulates an unreliable dependency, such as a third-party API that times out."""
    time.sleep(work)
    if random.random() < fail_rate:
        raise ConnectionError("upstream service timed out")
    return "ok"


@task()
def sleep(seconds: float) -> float:
    time.sleep(seconds)
    return seconds


@task()
def always_fails(message: str = "bad input") -> None:
    raise ValueError(message)
