"""Shared fixtures. Currently: a stub that behaves like S3.

The stub lives here rather than in one test module because two of them need it, and
a test that imports from another test module couples their collection order to their
contents. Fixtures are pytest's mechanism for exactly this.

It records ``put_order`` deliberately. The ``meta.json``-last invariant is a claim
about *sequence*, and a stub that only recorded final contents could not tell a
correct push from a backwards one.
"""

import base64
import hashlib

import pytest

from Scripts import paths, s3_store


def sha256_b64(data: bytes) -> str:
    """Base64 SHA-256, the form S3 reports ``ChecksumSHA256`` in."""
    return base64.b64encode(hashlib.sha256(data).digest()).decode()


class NoSuchKey(Exception):
    """What boto3 raises for a missing object, shaped enough for _is_missing."""

    response = {"Error": {"Code": "NoSuchKey"}}


class _Body:
    """The streaming body boto3 returns from get_object."""

    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


class _Paginator:
    """list_objects_v2's paginator, over the stub's dict."""

    def __init__(self, fake):
        self.fake = fake

    def paginate(self, *, Bucket, Prefix):
        yield {"Contents": [
            {"Key": key,
             "ETag": f'"{hashlib.md5(value).hexdigest()}"',
             "Size": len(value),
             "LastModified": None}
            for key, value in sorted(self.fake.objects.items())
            if key.startswith(Prefix)
        ]}


class FakeS3:
    """An in-memory S3 that records call order and wire reads."""

    def __init__(self):
        self.objects = {}          # key -> bytes
        self.put_order = []        # keys, in the order they were written
        self.get_calls = []        # keys actually fetched, for cache assertions

    def put_object(self, *, Bucket, Key, Body, ChecksumAlgorithm=None):
        data = Body.read() if hasattr(Body, "read") else Body
        self.objects[Key] = data
        self.put_order.append(Key)
        return {"ChecksumSHA256": sha256_b64(data)}

    def get_object(self, *, Bucket, Key):
        self._require(Key)
        self.get_calls.append(Key)
        return {"Body": _Body(self.objects[Key])}

    def head_object(self, *, Bucket, Key, ChecksumMode=None):
        self._require(Key)
        data = self.objects[Key]
        return {"ETag": f'"{hashlib.md5(data).hexdigest()}"',
                "ChecksumSHA256": sha256_b64(data),
                "ContentLength": len(data)}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _Paginator(self)

    def _require(self, key):
        if key not in self.objects:
            raise NoSuchKey()


@pytest.fixture
def s3_stub(monkeypatch, tmp_path):
    """A stubbed S3 client, a scratch store root, and a scratch ETag cache.

    Returns:
        FakeS3: The stub, for asserting on what was written and in what order.
    """
    stub = FakeS3()
    monkeypatch.setattr(s3_store, "client", lambda: stub)
    monkeypatch.setattr(s3_store, "CACHE_DIR", tmp_path / ".s3cache")
    monkeypatch.setattr(paths, "STORE_DIR", tmp_path / "Store")
    return stub


@pytest.fixture
def s3_env(s3_stub, monkeypatch, tmp_path):
    """:func:`s3_stub` plus a scratch ``Data/`` root and a pinned season.

    The mirrored tiers resolve against ``paths.DATA_DIR``, so anything exercising
    them needs it redirected or it walks the real repo.
    """
    from Scripts import sync

    monkeypatch.setattr(paths, "DATA_DIR", tmp_path)
    monkeypatch.setattr(sync, "get_season", lambda: 2026)
    return s3_stub
