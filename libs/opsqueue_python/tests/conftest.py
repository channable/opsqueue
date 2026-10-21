import cbor2
import pickle
import faulthandler
import signal
import sys
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


def _stack_dump_path(pid: int = os.getpid()) -> Path:
    return Path(f"/tmp/opsqueue-pytest-stack-{pid}.log")


def _linux_descendant_pids(parent_pid: int = os.getpid()) -> dict[int, tuple[str, str]]:
    """
    Returns a dictionary mapping descendant PIDs to their command name and command line. This is a
    recursive function that will find all descendants of the given parent PID. The command name and
    command line are read from the `/proc` filesystem, which is Linux-specific.
    """
    # The `/proc/{pid}/task/{tid}/children` file contains a space-separated list of child PIDs for
    # the given task. This interface is also not completely stable while there are running
    # processes. It can omit child PIDs if any of the child processes exits while reading the file.
    #
    # The canonical way is to read all `/proc/{pid}/stat` and reconstruct the tree in memory. As
    # this is currently only used for debug logging on SIGTERM, we can accept the risk of missing
    # some child PIDs.
    children = (
        Path(f"/proc/{parent_pid}/task/{parent_pid}/children")
        .read_text(encoding="utf-8")
        .strip()
    )

    def _child_command(pid: int) -> tuple[str, str]:
        """
        Returns the command name and command line of the given process id (pid).
        """
        try:
            comm = (
                Path(f"/proc/{pid}/comm").read_text(encoding="utf-8").strip()
                or "<comm>"
            )
        except FileNotFoundError:
            # Process might have exited between reading /proc/<pid>/children and now.
            comm = "<comm>"
        try:
            cmdline = (
                Path(f"/proc/{pid}/cmdline")
                .read_text(encoding="utf-8")
                .replace(
                    "\x00", " "
                )  # Replace null bytes with spaces before stripping.
                .strip()
                or "<cmdline>"
            )
        except FileNotFoundError:
            # Process might have exited between reading /proc/<pid>/children and now.
            cmdline = "<cmdline>"

        return (comm, cmdline)

    pids = [int(pid_text) for pid_text in children.split()]
    result = {pid: _child_command(pid) for pid in pids}
    for pid in pids:
        result.update(_linux_descendant_pids(pid))
    return result


def _handle_sigterm(signum: int, frame: object) -> None:
    """
    Handle `SIGTERM` by dumping the stack traces of all child processes that have registered the
    `SIGUSR1` handler (i.e. have a stack dump file). This is used for debugging tests that hang, as
    `pytest` will receive `SIGTERM` on the controller process when `timeout` fires. Do note that we
    will receive `SIGKILL` after a few seconds, so dump the stack traces quickly.
    """
    all_descendant_pids = _linux_descendant_pids(os.getpid())

    # Only signal processes that registered the `SIGUSR1` handler (i.e. have a dump file).
    # Internal `multiprocess` infrastructure processes (resource tracker, forkserver) do not
    # register the handler and have no dump file. Sending `SIGUSR1` to them with the default
    # disposition would kill them, which is undesirable.
    worker_pids = {
        pid: info
        for pid, info in all_descendant_pids.items()
        if _stack_dump_path(pid).exists()
    }

    if worker_pids:
        print(
            f"[opsqueue pytest] Requesting stack dump from child workers: {list(worker_pids.keys())}",
            file=sys.stderr,
            flush=True,
        )
        for pid in worker_pids.keys():
            try:
                os.kill(pid, signal.SIGUSR1)
            except ProcessLookupError:
                # Worker already exited.
                continue
            except Exception as exc:  # pragma: no cover - defensive diagnostics
                print(
                    f"[opsqueue pytest] Failed sending SIGUSR1 to child pid={pid} {worker_pids[pid]}: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
    else:
        print(
            "[opsqueue pytest] No child worker PIDs found at SIGTERM time",
            file=sys.stderr,
            flush=True,
        )

    # Dump the stack of the controller process itself, so we can see what it was doing at the time
    # of SIGTERM. This also provides the background processes enough time to dump their stacks
    # before the controller accesses those.
    faulthandler.dump_traceback(file=sys.stderr, all_threads=True)

    for pid in sorted(worker_pids.keys()):
        dump_path = _stack_dump_path(pid)
        print(
            f"[opsqueue pytest] ===== BEGIN CHILD STACK DUMP pid={pid} {worker_pids[pid]} ({dump_path}) =====",
            file=sys.stderr,
            flush=True,
        )
        try:
            print(dump_path.read_text(encoding="utf-8"), file=sys.stderr, flush=True)
        except Exception as exc:  # pragma: no cover - defensive diagnostics
            print(
                f"[opsqueue pytest] Failed reading dump file for child pid={pid} {worker_pids[pid]}: {exc}",
                file=sys.stderr,
                flush=True,
            )
        print(
            f"[opsqueue pytest] ===== END CHILD STACK DUMP pid={pid} {worker_pids[pid]} ({dump_path}) =====",
            file=sys.stderr,
            flush=True,
        )

    print(
        "[opsqueue pytest] ===== SIGTERM: End of diagnostic output =====",
        file=sys.stderr,
        flush=True,
    )
    os._exit(1)


def register_sigusr1_stack_dump_handler() -> None:
    """
    Make SIGUSR1 print a traceback in any process (controller, worker or background process).

    Registering this handler in subprocesses spawned by `multiprocess.Process` is a bit tricky, as
    the `multiprocess` module does not provide a hook for this and doesn't run any module imports in
    the child processes. We therefore register the handler at `_background_main` call instead.

    This is used for debugging tests that hang, as `timeout` will send SIGTERM to the controller
    and the controller will request stack dumps via `SIGUSR1` from all child processes that have
    created the stack dump file. The stack dumps will be written to already opened file when the
    process receives `SIGUSR1`, and the controller will read those files and print them to `stderr`.
    The file path is `/tmp/opsqueue-pytest-stack-<pid>.log`.
    """
    faulthandler.register(
        signal.SIGUSR1,
        file=_stack_dump_path().open("w", encoding="utf-8"),
        all_threads=True,
        chain=False,
    )


# Register signal handlers at module import time (before pytest_configure hook runs).
register_sigusr1_stack_dump_handler()
signal.signal(signal.SIGTERM, _handle_sigterm)


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:
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
    command_args: Iterable[str] = (),
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
        *command_args,
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


def _background_main(function: Callable[..., None], args: Iterable[Any]) -> None:
    """Entry point for background_processes.

    Registers the SIGUSR1 stack-dump handler before delegating to the real
    target function, so that the controller can request a traceback from this
    process when a timeout fires.
    """
    register_sigusr1_stack_dump_handler()
    function(*args)


@contextmanager
def background_process(
    function: Callable[..., None],
    args: Iterable[Any] = (),
) -> Generator[multiprocess.Process, None, None]:
    proc = multiprocess.Process(
        target=_background_main,
        args=(function, args),
        daemon=True,
    )
    try:
        proc.start()
        yield proc
    finally:
        proc.terminate()
        try:
            proc.join(timeout=1.0)
        except multiprocess.TimeoutError:
            proc.kill()  # Process can lock-up or ignore the termination signal, kill it.
            proc.join()  # Process should exit immediate after sending the kill signal.


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
