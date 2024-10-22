#!/usr/bin/env python

"""
A script for performing common operations, such as building, running tests and creating a release.
"""

from __future__ import annotations

import os
import subprocess
import sys

import click
from build_util import (
    nix,
)


@click.group()
def cli() -> None:
    """
    Perform common operations, such as building, running tests and creating a release
    """


@cli.group("build")
def cli_build() -> None:
    """
    Commands for building things
    """


@cli_build.command("opsqueue")
def cli_build_opsqueue() -> None:
    """
    Build opsqueue (the executable) through Nix
    """
    print(nix.build("nix/nixpkgs-pinned.nix", version=None, attribute="opsqueue"))


@cli_build.command("opsqueue_consumer")
def cli_build_opsqueue_consumer() -> None:
    """
    Build the opsqueue_consumer library through Nix
    """
    print(
        nix.build(
            "nix/nixpkgs-pinned.nix",
            version=None,
            attribute="pythonChannable.pkgs.opsqueue_consumer",
        )
    )


@cli_build.command("opsqueue_producer")
def cli_build_opsqueue_producer() -> None:
    """
    Build the opsqueue_producer library through Nix
    """
    print(
        nix.build(
            "nix/nixpkgs-pinned.nix",
            version=None,
            attribute="pythonChannable.pkgs.opsqueue_producer",
        )
    )


@cli.group("check")
def cli_check() -> None:
    """
    Run linters, optionally with automatic fixes.

    ./build.py check style [--fix]
    """


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


if __name__ == "__main__":
    # Build.py may be invoked from a different repository to run a command here,
    # but everything here expects to be running from the repository root, so
    # chdir there.
    os.chdir(os.path.dirname(__file__))
    cli()
