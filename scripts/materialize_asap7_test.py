#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

from dataclasses import replace
import gzip
import hashlib
import io
from pathlib import Path
import tarfile
import tempfile
import unittest

import materialize_asap7 as materializer


class TrackingResponse(io.BytesIO):
    def __init__(self, data):
        super().__init__(data)
        self.returned_bytes = 0

    def read(self, size=-1):
        data = super().read(size)
        self.returned_bytes += len(data)
        return data


class MaterializeAsap7Test(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.output = self.root / "bundle.tar.gz"
        self.members = []
        self.sources = {}
        self.contents = {}
        self.requests = []
        self.responses = []
        for name in ("AO.lib", "INVBUF.lib", "OA.lib", "SIMPLE.lib"):
            data = f"fixture {name}\n".encode()
            compressed = gzip.compress(data, mtime=0)
            url = f"https://fixtures.invalid/{name}.gz"
            self.sources[url] = compressed
            self.contents[name] = data
            self.members.append(
                materializer.Member(
                    name,
                    url,
                    hashlib.sha256(compressed).hexdigest(),
                    len(compressed),
                    hashlib.sha256(data).hexdigest(),
                    len(data),
                )
            )

    def opener(self, url, timeout):
        self.requests.append((url, timeout))
        response = TrackingResponse(self.sources[url])
        self.responses.append(response)
        return response

    def materialize(self, members=None, output=None):
        materializer.materialize(
            output or self.output,
            self.members if members is None else members,
            opener=self.opener,
        )

    def assert_rejected(self, member, message):
        with self.assertRaisesRegex(ValueError, message):
            self.materialize([member, *self.members[1:]])
        self.assertFalse(self.output.exists())

    def test_archive_matches_deterministic_fixture_and_metadata(self):
        self.materialize()
        actual = self.output.read_bytes()
        self.assertEqual(
            hashlib.sha256(actual).hexdigest(),
            "c5c11817ccd78304f731861b952e1f3991f1c6e11d10b360f3095e4a6147840f",
        )
        self.assertEqual(actual[:10], b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff")
        with tarfile.open(self.output, "r:gz") as archive:
            self.assertEqual(archive.getnames(), [member.name for member in self.members])
            for info in archive:
                self.assertTrue(info.isfile())
                self.assertEqual(info.mode, 0o644)
                self.assertEqual((info.uid, info.gid, info.mtime), (0, 0, 0))
                self.assertEqual((info.uname, info.gname), ("", ""))
                self.assertEqual(archive.extractfile(info).read(), self.contents[info.name])
        self.assertEqual(self.requests, [(member.url, 60) for member in self.members])
        second = self.root / "different-name.tar.gz"
        self.materialize(output=second)
        self.assertEqual(second.read_bytes(), actual)

    def test_compressed_checksum_is_verified_before_decompression(self):
        first = self.members[0]
        self.assert_rejected(replace(first, gzip_sha256="0" * 64), "compressed checksum")

    def test_uncompressed_checksum_is_verified(self):
        first = self.members[0]
        self.assert_rejected(
            replace(first, uncompressed_sha256="0" * 64), "uncompressed checksum"
        )

    def test_compressed_download_is_bounded_and_exact_size_is_required(self):
        first = self.members[0]
        self.assert_rejected(replace(first, gzip_bytes=1), "compressed byte count")
        self.assertEqual(self.responses[0].returned_bytes, 2)
        self.assert_rejected(
            replace(first, gzip_bytes=first.gzip_bytes + 1), "compressed byte count"
        )

    def test_decompression_requires_exact_size_and_rejects_expansion(self):
        first = self.members[0]
        for size in (1, first.uncompressed_bytes + 1):
            with self.subTest(size=size):
                self.assert_rejected(
                    replace(first, uncompressed_bytes=size), "uncompressed byte count"
                )

    def test_corrupt_gzip_payload_is_rejected_even_with_matching_compressed_pin(self):
        first = self.members[0]
        corrupted = bytearray(self.sources[first.url])
        corrupted[-8] ^= 1
        self.sources[first.url] = bytes(corrupted)
        members = [
            replace(first, gzip_sha256=hashlib.sha256(corrupted).hexdigest()),
            *self.members[1:],
        ]
        with self.assertRaises(gzip.BadGzipFile):
            self.materialize(members)
        self.assertFalse(self.output.exists())

    def test_invalid_metadata_is_rejected_before_downloading(self):
        first = self.members[0]
        cases = [
            replace(first, name="../AO.lib"),
            replace(first, name="nested/AO.lib"),
            replace(first, name="A" * 101),
            replace(first, name="\u00e9.lib"),
            replace(first, url="http://fixtures.invalid/AO.lib.gz"),
            replace(first, url="file:///fictional/AO.lib.gz"),
            replace(first, url="https:///missing-host"),
            replace(first, url="https://user:secret@fixtures.invalid/AO.lib.gz"),
            replace(first, gzip_sha256="a" * 63),
            replace(first, uncompressed_sha256="A" * 64),
            replace(first, gzip_bytes=0),
            replace(first, uncompressed_bytes=-1),
            replace(first, gzip_bytes=True),
        ]
        for member in cases:
            with self.subTest(member=member):
                with self.assertRaises(ValueError):
                    self.materialize([member, *self.members[1:]])
        for members in (
            self.members[:3],
            list(reversed(self.members)),
            [first, first, *self.members[2:]],
        ):
            with self.subTest(members=members):
                with self.assertRaises(ValueError):
                    self.materialize(members)
        self.assertEqual(self.requests, [])
        self.assertFalse(self.output.exists())

    def test_https_redirect_cannot_downgrade_to_http(self):
        handler = materializer.HttpsRedirectHandler()
        with self.assertRaisesRegex(ValueError, "HTTPS"):
            handler.redirect_request(
                None, None, 302, "Found", {}, "http://fixtures.invalid/library.gz"
            )

    def test_existing_output_is_never_overwritten(self):
        self.output.write_bytes(b"existing result")
        with self.assertRaises(FileExistsError):
            self.materialize()
        self.assertEqual(self.output.read_bytes(), b"existing result")


if __name__ == "__main__":
    unittest.main()
