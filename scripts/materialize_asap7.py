#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Reconstruct a deterministic ASAP7 archive from individually pinned sources."""

import argparse
from dataclasses import dataclass
import gzip
import hashlib
import io
from pathlib import Path
import re
import tarfile
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, build_opener


@dataclass(frozen=True)
class Member:
    name: str
    url: str
    gzip_sha256: str
    gzip_bytes: int
    uncompressed_sha256: str
    uncompressed_bytes: int


def validate_url(url):
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ValueError("ASAP7 source URL must use HTTPS without credentials or a fragment")


class HttpsRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        validate_url(new_url)
        return super().redirect_request(request, response, code, message, headers, new_url)


def open_https(url, timeout):
    return build_opener(HttpsRedirectHandler()).open(url, timeout=timeout)


def validate_members(members):
    if len(members) != 4:
        raise ValueError("ASAP7 archive requires exactly four source members")
    names = [member.name for member in members]
    if names != sorted(set(names)):
        raise ValueError("ASAP7 member names must be unique and sorted")
    for member in members:
        if (
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", member.name) is None
            or len(member.name) > 100
        ):
            raise ValueError("ASAP7 member name must be a safe USTAR basename")
        validate_url(member.url)
        for digest in (member.gzip_sha256, member.uncompressed_sha256):
            if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise ValueError("ASAP7 member checksum must be lowercase SHA-256")
        for size in (member.gzip_bytes, member.uncompressed_bytes):
            if type(size) is not int or size <= 0:
                raise ValueError("ASAP7 member byte count must be a positive integer")


def read_bounded(stream, limit):
    chunks = []
    remaining = limit
    while remaining:
        chunk = stream.read(min(remaining, 64 * 1024))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def verified_member(member, opener):
    with opener(member.url, timeout=60) as response:
        compressed = read_bounded(response, member.gzip_bytes + 1)
    if len(compressed) != member.gzip_bytes:
        raise ValueError(f"ASAP7 compressed byte count differs: {member.name}")
    if hashlib.sha256(compressed).hexdigest() != member.gzip_sha256:
        raise ValueError(f"ASAP7 compressed checksum differs: {member.name}")
    with gzip.GzipFile(fileobj=io.BytesIO(compressed), mode="rb") as source:
        data = read_bounded(source, member.uncompressed_bytes + 1)
    if len(data) != member.uncompressed_bytes:
        raise ValueError(f"ASAP7 uncompressed byte count differs: {member.name}")
    if hashlib.sha256(data).hexdigest() != member.uncompressed_sha256:
        raise ValueError(f"ASAP7 uncompressed checksum differs: {member.name}")
    return data


def materialize(output, members, opener=None):
    members = list(members)
    validate_members(members)
    if opener is None:
        opener = open_https
    contents = [verified_member(member, opener) for member in members]
    # A fresh attempt is required; never truncate an already published archive.
    with Path(output).open("xb") as destination:
        with gzip.GzipFile(
            filename="", mode="wb", fileobj=destination, compresslevel=9, mtime=0
        ) as compressed:
            with tarfile.open(
                fileobj=compressed, mode="w", format=tarfile.USTAR_FORMAT
            ) as archive:
                for member, data in zip(members, contents):
                    info = tarfile.TarInfo(member.name)
                    info.size = len(data)
                    info.mtime = 0
                    info.mode = 0o644
                    info.uid = info.gid = 0
                    info.uname = info.gname = ""
                    archive.addfile(info, io.BytesIO(data))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--member",
        action="append",
        nargs=6,
        required=True,
        metavar=("NAME", "URL", "GZIP_SHA256", "GZIP_BYTES", "SHA256", "BYTES"),
    )
    args = parser.parse_args()
    try:
        members = [
            Member(name, url, compressed_sha, int(compressed_size), sha, int(size))
            for name, url, compressed_sha, compressed_size, sha, size in args.member
        ]
        materialize(args.output, members)
    except (ValueError, OSError, EOFError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
