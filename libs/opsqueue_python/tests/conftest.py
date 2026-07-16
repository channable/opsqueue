import cbor2
import pickle
from contextlib import contextmanager, ExitStack
from typing import Generator, Callable, Any, Iterable
import multiprocess  # type: ignore[import-untyped]
import subprocess
import os
import pytest
from dataclasses import dataclass
from pathlib import Path
import functools

from opsqueue.common import SerializationFormat, json_as_bytes
from opsqueue.consumer import Strategy


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:
    print("A")
    multiprocess.set_start_method("forkserver")


PROJECT_ROOT = Path(__file__).parents[3]


@dataclass
class OpsqueueProcess:
    port: int
    process: subprocess.Popen[bytes]


@functools.cache
def opsqueue_bin_location() -> Path:
    if explicit_bin := os.environ.get("OPSQUEUE_BIN", "").strip():
        return Path(explicit_bin)
    else:
        subprocess.run(
            ["cargo", "build", "--quiet", "--bin", "opsqueue"],
            cwd=PROJECT_ROOT,
            check=True,
        )
        return PROJECT_ROOT / Path("target", "debug", "opsqueue")


@pytest.fixture
def opsqueue() -> Generator[OpsqueueProcess, None, None]:
    with opsqueue_service() as opsqueue_process:
        yield opsqueue_process


@contextmanager
def opsqueue_service(
    *,
    port: int = 0,  # The default of 0 means "pick any free port".
) -> Generator[OpsqueueProcess, None, None]:
    # This will create a SQLite database in memory.
    # We need the `cache=shared` to allow sharing this DB between all threads within the same OS process.
    temp_dbname = "file::memory:?cache=shared"

    # Switch this for the following if debugging a particular test locally.
    # This is not the default because specifically Semaphore backed with Butterfs
    # will from time to time hang for **many minutes** on initializing SQLite for some reason.
    # temp_dbname = f"/tmp/opsqueue_tests-{uuid.uuid4()}.db"

    read_fd, write_fd = os.pipe()

    command = [
        "setpriv",
        "--pdeathsig=SIGKILL",
        str(opsqueue_bin_location()),
        "--port",
        str(port),
        "--report-bound-port-pipe",
        str(write_fd),
        "--database-filename",
        temp_dbname,
    ]
    env = os.environ.copy()  # We copy the env so e.g. RUST_LOG and other env vars are propagated from outside of the invocation of pytest
    if env.get("RUST_LOG") is None:
        env["RUST_LOG"] = "off"

    try:
        with subprocess.Popen(
            command,
            cwd=PROJECT_ROOT,
            env=env,
            pass_fds=(write_fd,),
        ) as process:
            os.close(write_fd)
            write_fd = -1

            assert process.poll() is None, "Opsqueue process failed to start"
            try:
                actual_port = int.from_bytes(
                    read_exact_fd(read_fd, 2),
                    byteorder="big",
                    signed=False,
                )
                os.close(read_fd)
                read_fd = -1

                yield OpsqueueProcess(port=actual_port, process=process)
                assert process.poll() is None, "Opsqueue process failed during run"
            finally:
                # Give the process a chance to exit cleanly, but if it doesn't, kill it.
                # `with subprocess.Popen(...) as process` will not terminate the process on its own.
                process.terminate()
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired as exc:
                    process.kill()
                    raise AssertionError(
                        "Opsqueue process locked up for more than 1 second on shutdown"
                    ) from exc

    finally:
        if write_fd != -1:
            os.close(write_fd)
        if read_fd != -1:
            os.close(read_fd)


def read_exact_fd(fd: int, num_bytes: int) -> bytes:
    """
    Reads exactly `num_bytes` bytes from the given file descriptor `fd`.

    `os.read` may return fewer bytes than requested, so this function will keep reading until the
    requested number of bytes is obtained or EOF is reached.

    Raises EOFError if the end of the file is reached before reading the requested number of bytes.
    """
    data = bytearray()
    while len(data) < num_bytes:
        chunk = os.read(fd, num_bytes - len(data))
        if not chunk:
            raise EOFError(
                f"Unexpected EOF: expected {num_bytes} bytes, got {len(data)}: {bytes(data)!r}"
            )
        data.extend(chunk)
    return bytes(data)


@contextmanager
def background_process(
    function: Callable[..., None],
    args: Iterable[Any] = (),
) -> Generator[multiprocess.Process, None, None]:
    proc = multiprocess.Process(target=function, args=args)
    try:
        proc.daemon = True
        proc.start()
        yield proc
    finally:
        proc.terminate()


@contextmanager
def multiple_background_processes(
    function: Callable[[int], None], count: int
) -> Generator[list[multiprocess.Process], None, None]:
    with ExitStack() as stack:
        yield [
            stack.enter_context(background_process(function, args=(p,)))
            for p in range(count)
        ]


type StrategyDescription = str | tuple[str, str, StrategyDescription]

basic_strategies: Iterable[StrategyDescription] = ("Random", "Newest", "Oldest")
any_strategies: Iterable[StrategyDescription] = (
    *(basic_strategies),
    *(
        ("PreferDistinct", "id", s)
        for s in (
            *basic_strategies,
            *(("PreferDistinct", "second_id", s) for s in basic_strategies),
        )
    ),
)


@pytest.fixture(
    scope="function",
    ids=lambda s: f"Strategy.{strategy_from_description(s)}",
    params=basic_strategies,
)
def basic_consumer_strategy(
    request: pytest.FixtureRequest,
) -> Generator[StrategyDescription, None, None]:
    yield request.param


@pytest.fixture(
    scope="function",
    ids=lambda s: f"Strategy.{strategy_from_description(s)}",
    params=any_strategies,
)
def any_consumer_strategy(
    request: pytest.FixtureRequest,
) -> Generator[StrategyDescription, None, None]:
    yield request.param


@pytest.fixture(scope="function", params=[json_as_bytes, cbor2, pickle])
def serialization_format(
    request: pytest.FixtureRequest,
) -> Generator[SerializationFormat, None, None]:
    yield request.param


def strategy_from_description(description: StrategyDescription) -> Strategy:
    """
    PyO3 objects cannot currently be Pickle'd.
    This helper function allows us to pass a pickle-able description across `multiprocessing.Process` borders,
    and then look up the actual Strategy inside the consumer.
    """
    match description:
        case "Random":
            return Strategy.Random()
        case "Newest":
            return Strategy.Newest()
        case "Oldest":
            return Strategy.Oldest()
        case ("PreferDistinct", key, underlying):
            return Strategy.PreferDistinct(
                meta_key=key, underlying=strategy_from_description(underlying)
            )
