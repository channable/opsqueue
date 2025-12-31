import logging

import psutil
from opnieuw import retry
from psutil._common import pconn

LOGGER = logging.getLogger(__name__)


@retry(
    retry_on_exceptions=ValueError,
    max_calls_total=5,
    retry_window_after_first_call_in_seconds=5,
)
def wait_for_server(proc: psutil.Popen) -> tuple[str, int]:
    """
    Wait for a process to be listening on exactly one port and return that address.

    This function expects the given process to listen on a single port only. If the
    process is listening on no ports, a ValueError is raised and the check is retried.
    If multiple ports are listening, a RuntimeError is raised as this indicates an
    unexpected server configuration.
    """
    if not proc.is_running():
        raise ValueError(f"Process {proc} is not running")

    try:
        # Try to get the connections of the main process first, if that fails try the children.
        # Processes wrapped with `timeout` do not have connections themselves.
        connections: list[pconn] = (
            proc.net_connections()
            or [
                child_conn
                for child in proc.children(recursive=False)
                for child_conn in child.net_connections()
            ]
            or [
                child_conn
                for child in proc.children(recursive=True)
                for child_conn in child.net_connections()
            ]
        )
    except psutil.AccessDenied as e:
        match proc.status():
            case psutil.STATUS_ZOMBIE | psutil.STATUS_DEAD | psutil.STATUS_STOPPED:
                raise RuntimeError(f"Process {proc} has exited unexpectedly") from e
            case _:
                raise RuntimeError(
                    f"Could not get `net_connections` for process {proc}, access denied "
                ) from e

    ports = [x for x in connections if x.status == psutil.CONN_LISTEN]
    listen_count = len(ports)

    if listen_count == 0:
        raise ValueError(f"Process {proc} is not listening on any ports")
    if listen_count == 1:
        return ports[0].laddr

    raise RuntimeError(f"Process {proc} is listening on multiple ports")
