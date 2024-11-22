#!/usr/bin/env python

"""
A script for performing common operations, such as building, running tests and creating a release.
"""

from __future__ import annotations

import os
import subprocess
import sys

import click
from pathlib import Path
from build_util import (
    nix,
)


@click.group()
def cli() -> None:
    """
    Perform common operations, such as building, running tests and creating a release
    """


@cli.group("build", invoke_without_command=True)
@click.pass_context
def cli_build(ctx: click.Context) -> None:
    """
    Commands for building things
    """
    if ctx.invoked_subcommand is None:
        for cmd in cli_build.commands.values():
            ctx.invoke(cmd)


@cli_build.command("opsqueue")
def cli_build_opsqueue() -> None:
    """
    Build opsqueue (the executable) through Nix
    in the release profile
    """
    print(nix.build("nix/nixpkgs-pinned.nix", version=None, attribute="opsqueue"))


@cli_build.command("opsqueue_python")
def cli_build_opsqueue_python() -> None:
    """
    Build the opsqueue_consumer library through Nix
    in the release profile
    """
    print(nix_build_python_library())


def nix_build_python_library() -> Path:
    return nix.build(
        "nix/nixpkgs-pinned.nix",
        version=None,
        attribute="pythonChannable.pkgs.opsqueue_python",
    )


@cli.command("run")
@click.argument(
    "opsqueue-arguments",
    nargs=-1,
)
def cli_run(opsqueue_arguments: tuple[str]) -> None:
    """
    builds-and-runs the opsqueue executable,
    compiled from local sources using Cargo, with the development profile.

    To pass arguments to opsqueue and see more detailed help, use:
    `./build.py run -- --more --options="here"`

    Detailed help:

    `./build.py run -- --help
    """
    subprocess.run(("cargo", "run", "--bin", "opsqueue", "--") + opsqueue_arguments)


@cli.group("check", invoke_without_command=True)
@click.pass_context
def cli_check(ctx: click.Context) -> None:
    """
    Run linters, optionally with automatic fixes.

    When invoked without any subcommand, runs all checks.

    ./build.py check
    ./build.py check style [--fix]
    """
    if ctx.invoked_subcommand is None:
        for cmd in cli_check.commands.values():
            ctx.invoke(cmd)


@cli_check.command("style")
@click.option(
    "--fix/--no-fix",
    default=False,
    help="Whether to automatically apply fixes for the lints where possible",
)
def cli_check_style(fix: bool) -> None:
    """
    Run a set of linters.
    """
    if fix is False:
        pre_commit_config = ".pre-commit-config-check.yaml"
    else:
        pre_commit_config = ".pre-commit-config-fix.yaml"

    try:
        subprocess.run(
            ["pre-commit", "run", "-c", pre_commit_config, "--all-files"],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        click.secho(
            "Style check failed, please see pre-commit output."
            + (" Consider running with --fix" if fix is False else ""),
            fg="red",
            bold=True,
        )
        sys.exit(e.returncode)


@cli.group("test", invoke_without_command=True)
@click.pass_context
def cli_test(ctx) -> None:
    """
    Commands to run tests.

    When invoked with no subcommand, runs all test suites.
    """
    if ctx.invoked_subcommand is None:
        for cmd in cli_test.commands.values():
            ctx.invoke(cmd)


@cli_test.command("unit")
@click.argument(
    "test-arguments",
    nargs=-1,
)
def cli_test_unit(test_arguments: tuple[str]) -> None:
    """
    Runs unit tests (using `cargo test`).

    Extra arguments (after `--`) are forwarded to `cargo test`.
    """
    subprocess.run(("cargo", "test", "--all-features", "--") + test_arguments)


@cli_test.command("integration")
@click.option("--via-nix", is_flag=True, default=False)
@click.argument(
    "test-arguments",
    nargs=-1,
)
def cli_test_integration(via_nix: bool, test_arguments: tuple[str]) -> None:
    """
    Runs integration tests (using a specially prepared `pytest`)

    Use `--via-nix` to run the tests against
    a Nix-built opsqueue executable + Python FFI library.

    By default, instead we'll build the executable with `cargo`
    and run the FFI library with `maturin` inside a Python virtualenv.


    Extra arguments (after `--`) are forwarded to `pytest`.

    If you need to debug tests:

    * use pytest's `--log-cli-level=info` (or `=debug`) argument
    to get more detailed logs from the producer/consumer clients

    * use `RUST_LOG="opsqueue=info"`
    (or `opsqueue=debug` or `debug` for even more verbosity),
    together with to the pytest option `-s` AKA `--capture=no`,
    to debug the opsqueue binary itself.

    Example: `RUST_LOG="opsqueue=info" ./build.py test integration -- --log-cli-level=debug
    """
    if via_nix:
        print("Running integration tests via Nix...")
        python_lib_dir = (
            nix_build_python_library() / "lib" / "python3.12" / "site-packages"
        )
        preface = f"""
        set -e
        cd libs/opsqueue_python/tests

        export PYTHONPATH="{python_lib_dir}"
        export OPSQUEUE_VIA_NIX="true"
        export RUST_LOG="opsqueue=debug"
        """
    else:
        print("Running integration tests using local Cargo/Maturin/Venv (no Nix)...")
        preface = """
            set -e
            # Used by the integration tests
            # Make sure it's up-to-date now to not slow down the first test that calls it
            cargo build --bin opsqueue
            cd libs/opsqueue_python
            source "./.setup_local_venv.sh"

            maturin develop
        """
    command = f"pytest --color=yes {" ".join(test_arguments)}"
    subprocess.check_call(preface + command, shell=True)


@cli_test.command("nixtegration")
@click.argument(
    "test-arguments",
    nargs=-1,
)
def cli_test_nixtergration(test_arguments: tuple[str]) -> None:
    python_lib_dir = nix.build(
        "nix/nixpkgs-pinned.nix",
        version=None,
        attribute="pythonChannable.pkgs.opsqueue_python",
    )
    python_lib_dir = python_lib_dir / "lib" / "python3.12" / "site-packages"
    command = f"""
    set -e
    cd libs/opsqueue_python/tests

    export PYTHONPATH="{python_lib_dir}"
    export OPSQUEUE_VIA_NIX="true"
    export RUST_LOG="opsqueue=debug"
    pytest --color=yes -vvvvv -s --log-cli-level=debug
    """
    subprocess.check_call(command, shell=True)


if __name__ == "__main__":
    # Build.py may be invoked from a different repository to run a command here,
    # but everything here expects to be running from the repository root, so
    # chdir there.
    os.chdir(os.path.dirname(__file__))
    cli()
