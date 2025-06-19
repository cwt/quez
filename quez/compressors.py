"""
Defines the Compressor protocol and provides concrete implementations
for various compression algorithms.
"""
import zlib
import bz2
import lzma
from typing import Protocol, runtime_checkable


@runtime_checkable
class Compressor(Protocol):
    """
    A protocol defining the interface for a pluggable compression strategy.
    Any object that implements a `compress` and `decompress` method can
    be used as a compressor.
    """
    def compress(self, data: bytes) -> bytes:
        """Compresses the input bytes and returns compressed bytes."""
        ...

    def decompress(self, data: bytes) -> bytes:
        """Decompresses the input bytes and returns original bytes."""
        ...


class ZlibCompressor:
    """A compressor strategy using the zlib library (default)."""
    def compress(self, data: bytes) -> bytes:
        return zlib.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return zlib.decompress(data)


class Bz2Compressor:
    """A compressor strategy using the bz2 library for higher compression."""
    def compress(self, data: bytes) -> bytes:
        return bz2.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return bz2.decompress(data)


class LzmaCompressor:
    """A compressor strategy using the lzma library for very high compression."""
    def compress(self, data: bytes) -> bytes:
        return lzma.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return lzma.decompress(data)


class NullCompressor:
    """A pass-through strategy that performs no compression."""
    def compress(self, data: bytes) -> bytes:
        return data

    def decompress(self, data: bytes) -> bytes:
        return data

