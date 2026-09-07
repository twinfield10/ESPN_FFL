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


#: The real store, resolved once at import before any test can redirect it.
_REAL_STORE_ROOT = paths.STORE_DIR


@pytest.fixture(autouse=True)
def never_write_the_real_store(monkeypatch):
    """Make it impossible for a test to write into ``Data/Store``.

    **This exists because a test destroyed the real store.** ``test_freeze.py``'s
    fixture redirected ``paths.DATA_DIR`` and not ``paths.STORE_DIR`` -- and
    ``STORE_DIR`` is computed from ``DATA_DIR`` *at import*, so the patch moved
    nothing. Three leagues' ``board.parquet`` (2.2 MB each), ``draft.parquet`` and
    ``meta.json`` were overwritten with three-row test frames, on the afternoon of
    two live drafts. They came back from S3, which is what the system of record is
    for, but nothing in the suite objected at the time -- the only symptom was two
    unrelated tests in ``test_lab_g2.py`` failing on the wreckage a run later.

    Redirecting the store is still the test's own job. This only ensures that
    forgetting to fails loudly and immediately, instead of silently succeeding
    against real data. Autouse, because the tests that need protecting are exactly
    the ones that did not know they did.
    """
    from Scripts import store as scripts_store

    real = scripts_store.write_league_store

    def guarded(season, league_key, **kwargs):
        if paths.store_root() == _REAL_STORE_ROOT:
            raise AssertionError(
                f"a test tried to write the real store at {_REAL_STORE_ROOT}. "
                f"Redirect it first: monkeypatch.setattr(paths, 'STORE_DIR', "
                f"tmp_path / 'Store'). Patching DATA_DIR is not enough -- STORE_DIR "
                f"is derived from it at import."
            )
        return real(season, league_key, **kwargs)

    monkeypatch.setattr(scripts_store, "write_league_store", guarded)
