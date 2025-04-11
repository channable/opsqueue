import cbor2
import pickle
from contextlib import contextmanager, ExitStack
from typing import Generator, Callable, Any, Iterable
import multiprocessing
import subprocess
import uuid
import os
import pytest
from dataclasses import dataclass
from pathlib import Path
import functools

from opsqueue.common import SerializationFormat, json_as_bytes
from opsqueue.consumer import Strategy

# @pytest.hookimpl(tryfirst=True)
# def pytest_configure(config: pytest.Config) -> None:
#     print("A")
#     multiprocessing.set_start_method('forkserver')


@dataclass
class OpsqueueProcess:
    port: int
    process: subprocess.Popen[bytes]


@functools.cache
def opsqueue_bin_location() -> Path:
    if os.environ.get("OPSQUEUE_VIA_NIX"):
        deriv_path = (
            subprocess.check_output(["just", "nix-build-bin"]).decode("utf-8").strip()
        )
        return Path(deriv_path) / "bin" / "opsqueue"
    else:
        subprocess.run(["cargo", "build", "--quiet", "--bin", "opsqueue"])
        return Path(".", "target", "debug", "opsqueue")


@pytest.fixture
def opsqueue() -> Generator[OpsqueueProcess, None, None]:
    with opsqueue_service() as opsqueue_process:
        yield opsqueue_process


@contextmanager
def opsqueue_service(
    *, port: int | None = None
) -> Generator[OpsqueueProcess, None, None]:
    global test_opsqueue_port_offset

    if port is None:
        port = random_free_port()

    temp_dbname = f"/tmp/opsqueue_tests-{uuid.uuid4()}.db"

    command = [
        str(opsqueue_bin_location()),
        "--port",
        str(port),
        "--database-filename",
        temp_dbname,
    ]
    cwd = "../../"
    env = os.environ.copy()  # We copy the env so e.g. RUST_LOG and other env vars are propagated from outside of the invocation of pytest
    if env.get("RUST_LOG") is None:
        env["RUST_LOG"] = "off"

    with subprocess.Popen(command, cwd=cwd, env=env) as process:
        try:
            wrapper = OpsqueueProcess(port=port, process=process)
            yield wrapper
        finally:
            process.terminate()


def random_free_port() -> int:
    import random

    while True:
        port = random.randrange(10_000, 60_000)
        if not is_port_in_use(port):
            return port


def is_port_in_use(port: int) -> bool:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0


@contextmanager
def background_process(
    function: Callable[..., None],
    args: Iterable[Any] = (),
) -> Generator[multiprocessing.Process, None, None]:
    proc = multiprocessing.Process(target=function, args=args)
    try:
        proc.daemon = True
        proc.start()
        yield proc
    finally:
        proc.terminate()


@contextmanager
def multiple_background_processes(
    function: Callable[[int], None], count: int
) -> Generator[None, None, None]:
    with ExitStack() as stack:
        for p in range(count):
            stack.enter_context(background_process(function, args=(p,)))
        yield


basic_strategies = Strategy.Random(), Strategy.Newest(), Strategy.Oldest()
any_strategies = [
    *basic_strategies,
    *(Strategy.PreferDistinct(meta_key="id", underlying=s) for s in basic_strategies),
]


@pytest.fixture(
    scope="function",
    ids=lambda s: f"Strategy.{s}",
    params=basic_strategies,
)
def basic_consumer_strategy(
    request: pytest.FixtureRequest,
) -> Generator[Strategy, None, None]:
    yield request.param


@pytest.fixture(
    scope="function",
    ids=lambda s: f"Strategy.{s}",
    params=any_strategies,
)
def any_consumer_strategy(
    request: pytest.FixtureRequest,
) -> Generator[Strategy, None, None]:
    yield request.param


@pytest.fixture(scope="function", params=[json_as_bytes, cbor2, pickle])
def serialization_format(
    request: pytest.FixtureRequest,
) -> Generator[SerializationFormat, None, None]:
    yield request.param
