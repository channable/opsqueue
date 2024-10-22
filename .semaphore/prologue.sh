#!/bin/bash

# Shared commands for the set-up phase of jobs

# This file is NOT usable with Semaphore's "commands_file" property, as this
# puts all the comments in this file in the build output and requires us to
# write all build steps on one line each. Instead, this file must be executed
# (not sourced) in the build pipelines where we use it.
# If the build requires the use of Nix, this command must be rerun afterwards:
# `source $HOME/.nix-profile/etc/profile.d/nix.sh`
# The reason for executing and not sourcing this file (and having to re-source
# nix.sh) is that sourcing interferes with Semaphore's job control flow: if a
# command would fail in this pipeline, it immediately causes Semaphore to abort
# the job, without running any cleanup commands or the epilogue (which we use
# to report failures to Healthchecks.io).

# We don't use `set -x` here to show the run commands, because this would show
# authentication tokens in the build output. Instead, `set -v` prints the
# commands "as they are read".
# We do want to exit immediately after encountering an error.
# We also cannot use "set -u", as Semaphore's tooling uses unset variables in
# some places (which we can't change).
set -evo pipefail

# Install Nix. We install in single-user mode (--no-daemon) because the Nix
# process can access the running SSH agent to fetch private Git repositories.
curl -o install-nix https://releases.nixos.org/nix/nix-2.24.9/install
sha256sum --check .semaphore/install-nix.sha256

# Hotfix for Semaphore issue preventing installing Nix
unset LD_LIBRARY_PATH

mount_nix_store() {
  # Before we can successfully restore to `/nix` it needs to be created and owned
  # by the CI user. Without this, the `cache restore` command fails because it
  # doesn't have permission to create `/nix`. (We cannot run the cache restore
  # command as `root` as it takes settings from environment variables.)
  # We use the local scratch SSD mounted at `/mnt` to prevent running out of disk
  # space, as the Nix store can get quite large.
  sudo mkdir -p /mnt/nix /nix
  sudo mount --bind /mnt/nix /nix
  sudo chown -R semaphore: /nix
}
mount_nix_store

# Attempt to restore the Semaphore cache entry for `/nix`.
#
# We have this in addition to Cachix because we want to avoid hitting Cachix
# for individual store entries, as restoring `/nix` from the Semaphore cache in
# one go is a lot faster than downloading individual cache entries from
# Cachix's S3 + Cloudflare.
#
# We refresh the Nix store cache entry daily. It is populated after the first
# successful build of the day by our main pipeline.
#
# Restoring the cache can fail when the cache entry is only partially matched,
# because then it might still be in the process of being uploaded by Semaphore,
# which can be caused by a concurrent build. Since using the Semaphore cache is
# only an optimization, but not strictly necessary, we make sure that the build
# doesn't fail in this case, by making sure the exit code is always 0. When
# restoring the cache fails, we delete /nix to ensure that we are not left with
# a partially restored Nix store.
cache restore "nix-store-$(date -u -Idate)" || {
  sudo umount /nix
  sudo rm -fr /mnt/nix
  mount_nix_store
}

# Clean-up old profiles to get a fresher restore. Also avoids issues arising
# from the `nix-env` -> `nix profile` transition.
rm -rf /nix/var/nix/profiles/

# Install nix
sh ./install-nix --no-daemon

sudo mkdir /etc/nix
echo 'experimental-features = nix-command' | sudo tee -a /etc/nix/nix.conf

# Activate nix profile
# Disable shellcheck, because the file does not exist before this script is run.
# shellcheck disable=SC1091
source "$HOME/.nix-profile/etc/profile.d/nix.sh"

# Enable cachix. Cachix is also in default.nix, but it is installed separately
# here because it is needed for building default.nix.
nix-env -iA nixpkgs.cachix
cachix use channable
cachix use channable-public

# Place the result in a symlink, so it can be accessed both in the prologue and
# in the job without re-evaluating. This command is instantaneous because the
# derivation is already built.
nix-store --realize --add-root "$HOME/locale_archive"
LOCALE_ARCHIVE=$(readlink -f "$HOME/locale_archive")/lib/locale/locale-archive
export LOCALE_ARCHIVE

# Enable building multiple derivations at the same time. See the max-jobs option in
# https://nixos.org/manual/nix/unstable/command-ref/opt-common.html
echo "max-jobs = auto" >> ~/.config/nix/nix.conf
# Enable using multiple cores for a single derivation at the same time. See the cores option in
# https://nixos.org/manual/nix/unstable/command-ref/opt-common.html
echo "cores = 0" >> ~/.config/nix/nix.conf
