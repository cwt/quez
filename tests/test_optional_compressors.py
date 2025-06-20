"""
Tests for optional compressors (zstd, lzo).
These tests will be skipped if the required libraries are not installed.
"""

import pytest

from quez import AsyncCompressedQueue, CompressedQueue

# --- Zstandard Tests (skipped if zstandard is not installed) ---
zstandard = pytest.importorskip(
    "zstandard", reason="zstandard library not found"
)
from quez.compressors import ZstdCompressor  # type: ignore


class TestZstdCompressor:
    """Tests for the ZstdCompressor."""

    def test_compress_decompress_roundtrip(self):
        """Test the basic compress/decompress roundtrip."""
        compressor = ZstdCompressor()
        original_data = b"some data to compress with zstd" * 10
        compressed = compressor.compress(original_data)
        decompressed = compressor.decompress(compressed)
        assert original_data == decompressed
        assert len(compressed) < len(original_data)

    def test_sync_queue_with_zstd(self):
        """Test that ZstdCompressor works with the synchronous queue."""
        q = CompressedQueue(compressor=ZstdCompressor())
        item = {"payload": "This is a test with ZSTD" * 10}
        q.put(item)
        stats = q.stats
        assert stats["count"] == 1
        assert stats["raw_size_bytes"] > stats["compressed_size_bytes"]
        retrieved = q.get()
        assert item == retrieved

    @pytest.mark.asyncio
    async def test_async_queue_with_zstd(self):
        """Test that ZstdCompressor works with the asynchronous queue."""
        q = AsyncCompressedQueue(compressor=ZstdCompressor())
        item = {"payload": "This is an async test with ZSTD" * 10}
        await q.put(item)
        stats = q.stats
        assert stats["count"] == 1
        assert stats["raw_size_bytes"] > stats["compressed_size_bytes"]
        retrieved = await q.get()
        assert item == retrieved


# --- LZO Tests (skipped if python-lzo is not installed) ---
lzo = pytest.importorskip("lzo", reason="python-lzo library not found")
from quez.compressors import LzoCompressor  # type: ignore


class TestLzoCompressor:
    """Tests for the LzoCompressor."""

    def test_compress_decompress_roundtrip(self):
        """Test the basic compress/decompress roundtrip."""
        compressor = LzoCompressor()
        original_data = b"some data to compress with lzo" * 10
        compressed = compressor.compress(original_data)
        decompressed = compressor.decompress(compressed)
        assert original_data == decompressed
        assert len(compressed) < len(original_data)

    def test_sync_queue_with_lzo(self):
        """Test that LzoCompressor works with the synchronous queue."""
        q = CompressedQueue(compressor=LzoCompressor())
        item = {"payload": "This is a test with LZO" * 10}
        q.put(item)
        stats = q.stats
        assert stats["count"] == 1
        assert stats["raw_size_bytes"] > stats["compressed_size_bytes"]
        retrieved = q.get()
        assert item == retrieved

    @pytest.mark.asyncio
    async def test_async_queue_with_lzo(self):
        """Test that LzoCompressor works with the asynchronous queue."""
        q = AsyncCompressedQueue(compressor=LzoCompressor())
        item = {"payload": "This is an async test with LZO" * 10}
        await q.put(item)
        stats = q.stats
        assert stats["count"] == 1
        assert stats["raw_size_bytes"] > stats["compressed_size_bytes"]
        retrieved = await q.get()
        assert item == retrieved
