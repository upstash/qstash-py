import asyncio
import os
import time
from typing import Callable, Coroutine

import dotenv

QSTASH_TOKEN = os.environ.get(
    "QSTASH_TOKEN",
    dotenv.dotenv_values().get("QSTASH_TOKEN"),
)

QSTASH_CURRENT_SIGNING_KEY = os.environ.get(
    "QSTASH_CURRENT_SIGNING_KEY",
    dotenv.dotenv_values().get("QSTASH_CURRENT_SIGNING_KEY"),
)

QSTASH_NEXT_SIGNING_KEY = os.environ.get(
    "QSTASH_NEXT_SIGNING_KEY",
    dotenv.dotenv_values().get("QSTASH_NEXT_SIGNING_KEY"),
)


def assert_eventually(
    assertion: Callable[[], None],
    initial_delay: float = 0,
    retry_delay: float = 0.5,
    timeout: float = 10.0,
) -> None:
    if initial_delay > 0:
        time.sleep(initial_delay)

    deadline = time.time() + timeout
    last_err = None

    while time.time() < deadline:
        try:
            assertion()
            return
        except AssertionError as e:
            last_err = e
            time.sleep(retry_delay)

    if last_err is None:
        raise AssertionError("Couldn't run the assertion")

    raise last_err


async def assert_eventually_async(
    assertion: Callable[[], Coroutine[None, None, None]],
    initial_delay: float = 0,
    retry_delay: float = 0.5,
    timeout: float = 10.0,
) -> None:
    if initial_delay > 0:
        await asyncio.sleep(initial_delay)

    deadline = time.time() + timeout
    last_err = None

    while time.time() < deadline:
        try:
            await assertion()
            return
        except AssertionError as e:
            last_err = e
            await asyncio.sleep(retry_delay)

    if last_err is None:
        raise AssertionError("Couldn't run the assertion")

    raise last_err
