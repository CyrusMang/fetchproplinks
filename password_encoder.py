#!/usr/bin/env python3
"""Generate Argon2 password hashes compatible with this project's auth settings.

Auth parameters mirrored from actions/auth.ts:
- memoryCost: 19456
- timeCost: 2
- outputLen: 32
- parallelism: 1
"""

from __future__ import annotations

import argparse
import getpass
import os
import secrets
import sys

try:
    from argon2.low_level import Type, hash_secret
    from argon2.exceptions import HashingError
except ImportError:
    print("Missing dependency: argon2-cffi")
    print("Install with: python3 -m pip install argon2-cffi")
    sys.exit(1)

MEMORY_COST = 19456
TIME_COST = 2
HASH_LEN = 32
PARALLELISM = 1
SALT_LEN = 16
MIN_SALT_LEN = 8


def encode_password(password: str, salt: bytes | None = None) -> tuple[str, bytes]:
    if salt is None:
        salt = secrets.token_bytes(SALT_LEN)
    elif len(salt) < MIN_SALT_LEN:
        raise ValueError(
            f"Salt too short ({len(salt)} bytes). Argon2 requires at least {MIN_SALT_LEN} bytes."
        )

    try:
        encoded = hash_secret(
            secret=password.encode("utf-8"),
            salt=salt,
            time_cost=TIME_COST,
            memory_cost=MEMORY_COST,
            parallelism=PARALLELISM,
            hash_len=HASH_LEN,
            type=Type.ID,
            version=19,
        ).decode("utf-8")
    except HashingError as exc:
        raise ValueError(f"Argon2 hashing failed: {exc}") from exc

    return encoded, salt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Encode a password with Argon2id using the app's auth parameters."
    )
    parser.add_argument(
        "--password",
        help="Plain-text password. If omitted, you will be prompted securely.",
    )
    parser.add_argument(
        "--salt-hex",
        help="Optional salt in hex. If omitted, a random salt is generated.",
    )
    parser.add_argument(
        "--print-salt",
        action="store_true",
        help="Print salt hex value (useful for deterministic regeneration/testing).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    password = args.password
    if not password:
        password = getpass.getpass("Password: ")

    if not password:
        print("Password cannot be empty.")
        return 1

    salt = None
    if args.salt_hex:
        try:
            salt = bytes.fromhex(args.salt_hex)
        except ValueError:
            print("Invalid --salt-hex value. Must be valid hex.")
            return 1

    try:
        encoded, salt_used = encode_password(password=password, salt=salt)
    except ValueError as exc:
        print(str(exc))
        return 1

    print(encoded)
    if args.print_salt:
        print(f"salt_hex={salt_used.hex()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
