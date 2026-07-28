#!/usr/bin/env python3
"""Patch the OPSQUEUE_TAG: marker in a compiled opsqueue binary.

Usage: stamp-tag.py <binary-path> <tag>

The binary must contain the 64-byte OPSQUEUE_RELEASE_TAG symbol:
  - Bytes  0..12  : b'OPSQUEUE_TAG:' (marker, unchanged)
  - Bytes 13..63  : tag value, null-padded to 51 bytes (patched here)

This script is called by the Nix `withTag` build to stamp a release tag
into the pre-compiled binary without recompiling Rust.
"""

import sys

MARKER = b"OPSQUEUE_TAG:"
TOTAL_FIELD_LEN = 64
TAG_FIELD_LEN = TOTAL_FIELD_LEN - len(MARKER)  # 51 bytes


def main() -> None:
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <binary-path> <tag>", file=sys.stderr)
        sys.exit(1)

    binary_path = sys.argv[1]
    tag = sys.argv[2]

    tag_bytes = tag.encode("utf-8")
    if len(tag_bytes) > TAG_FIELD_LEN:
        print(
            f"Error: tag is {len(tag_bytes)} bytes, max is {TAG_FIELD_LEN}",
            file=sys.stderr,
        )
        sys.exit(1)

    with open(binary_path, "rb") as f:
        data = f.read()

    pos = data.find(MARKER)
    if pos == -1:
        print(f"Error: marker {MARKER!r} not found in {binary_path}", file=sys.stderr)
        sys.exit(1)

    second = data.find(MARKER, pos + 1)
    if second != -1:
        print(
            f"Error: marker {MARKER!r} found more than once (at {pos} and {second})",
            file=sys.stderr,
        )
        sys.exit(1)

    padded_tag = tag_bytes.ljust(TAG_FIELD_LEN, b"\x00")
    patched = data[: pos + len(MARKER)] + padded_tag + data[pos + TOTAL_FIELD_LEN :]

    if len(patched) != len(data):
        print("Error: patched binary has a different size", file=sys.stderr)
        sys.exit(1)

    with open(binary_path, "wb") as f:
        f.write(patched)

    print(f"Stamped tag {tag!r} into {binary_path}")


if __name__ == "__main__":
    main()
