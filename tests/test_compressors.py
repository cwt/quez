"""
Tests for the standalone Compressor and Serializer implementations.
"""

import pytest
from quez.compressors import (
    Bz2Compressor,
    Compressor,
    LzmaCompressor,
    NullCompressor,
    PickleSerializer,
    ZlibCompressor,
)

# --- Test Data ---
SAMPLE_OBJECTS = [
    {"message": "hello world", "id": 123, "data": [1, 2.0, "three"]},
    "a simple string",
    b"some raw bytes",
    12345,
    [{"a": 1}, {"b": 2}],
    None,
]

COMPRESSORS = [
    ZlibCompressor(),
    Bz2Compressor(),
    LzmaCompressor(),
    NullCompressor(),
]


# --- Tests for Serializer ---
class TestPickleSerializer:
    @pytest.mark.parametrize("obj", SAMPLE_OBJECTS)
    def test_dumps_and_loads_roundtrip(self, obj):
        """
        Tests that serializing and then deserializing an object
        returns the original object.
        """
        serializer = PickleSerializer()
        serialized_data = serializer.dumps(obj)
        deserialized_obj = serializer.loads(serialized_data)

        assert isinstance(serialized_data, bytes)
        assert deserialized_obj == obj


# --- Tests for Compressors ---
@pytest.mark.parametrize("compressor", COMPRESSORS)
class TestCompressors:
    def test_compress_decompress_roundtrip(self, compressor: Compressor):
        """
        Tests that compressing and then decompressing data
        returns the original data.
        """
        original_data = (
            b"This is some sample data for testing compression algorithms." * 10
        )
        compressed_data = compressor.compress(original_data)
        decompressed_data = compressor.decompress(compressed_data)

        assert isinstance(compressed_data, bytes)
        assert original_data == decompressed_data

    def test_compress_empty_bytes(self, compressor: Compressor):
        """Tests that compressors correctly handle empty byte strings."""
        original_data = b""
        compressed_data = compressor.compress(original_data)
        decompressed_data = compressor.decompress(compressed_data)

        assert original_data == decompressed_data
