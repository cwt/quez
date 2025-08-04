"""
Defines the Compressor protocol and provides concrete implementations
for various compression algorithms. It also includes a Serializer protocol
to handle the conversion between arbitrary Python objects and bytes.
"""

from typing import Any, Protocol, runtime_checkable
import bz2
import lzma
import pickle
import zlib


# --- 1. Serializer Protocol and Default Implementation ---
@runtime_checkable
class Serializer(Protocol):
    """
    A protocol for serializing objects to bytes and deserializing back.
    This allows the queue to handle any pickle-able Python object, not
    just bytes.
    """

    def dumps(self, obj: Any) -> bytes:
        """Serializes the object into bytes."""
        ...

    def loads(self, data: bytes) -> Any:
        """Deserializes bytes back into an object."""
        ...


class PickleSerializer:
    """
    The default serializer using the pickle module.
    It can handle most Python objects.
    """

    def dumps(self, obj: Any) -> bytes:
        """Serializes the given object into bytes using pickle."""
        return pickle.dumps(obj)

    def loads(self, data: bytes) -> Any:
        """Deserializes bytes back into an object using pickle."""
        return pickle.loads(data)


# --- 2. Compressor Protocol and Implementations ---
@runtime_checkable
class Compressor(Protocol):
    """
    A protocol defining the interface for a pluggable compression strategy.
    Any object that implements `compress` and `decompress` methods can
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

    def __init__(self, level: int = -1) -> None:
        """
        Initializes the compressor with a specific compression level.

        The level can be between 0 (no compression) and 9 (best compression).
        If not specified, it defaults to -1, which means the default compression.
        """
        self.level = level

    def compress(self, data: bytes) -> bytes:
        """Compresses the input bytes using zlib and returns compressed bytes."""
        return zlib.compress(data, level=self.level)

    def decompress(self, data: bytes) -> bytes:
        """Decompresses the input bytes using zlib and returns original bytes."""
        return zlib.decompress(data)


class Bz2Compressor:
    """A compressor strategy using the bz2 library for higher compression."""

    def __init__(self, level: int = 9) -> None:
        """
        Initializes the compressor with a specific compression level.

        The level can be between 0 (no compression) and 9 (best compression).
        If not specified, it defaults to 9, which is the highest compression.
        """
        self.level = level

    def compress(self, data: bytes) -> bytes:
        """Compresses the input bytes using bz2 and returns compressed bytes."""
        return bz2.compress(data, compresslevel=self.level)

    def decompress(self, data: bytes) -> bytes:
        """Decompresses the input bytes using bz2 and returns original bytes."""
        return bz2.decompress(data)


class LzmaCompressor:
    """A compressor strategy using the lzma library for very high compression."""

    def __init__(self, level: int = lzma.PRESET_DEFAULT) -> None:
        """
        Initializes the compressor with a specific preset level.

        The preset can be between 0 (no compression) and 9 (best compression).
        If not specified, it defaults to lzma.PRESET_DEFAULT.
        """
        self.level = level

    def compress(self, data: bytes) -> bytes:
        """Compresses the input bytes using lzma and returns compressed bytes."""
        return lzma.compress(data, preset=self.level)

    def decompress(self, data: bytes) -> bytes:
        """Decompresses the input bytes using lzma and returns original bytes."""
        return lzma.decompress(data)


class NullCompressor:
    """A pass-through strategy that performs no compression."""

    def compress(self, data: bytes) -> bytes:
        """Returns the input bytes unchanged, simulating no compression."""
        return data

    def decompress(self, data: bytes) -> bytes:
        """Returns the input bytes unchanged, simulating no decompression."""
        return data


# --- 3. Optional Compressors ---

# ZstdCompressor is only available if the 'zstandard' library is installed.
try:
    import zstandard  # type: ignore

    class ZstdCompressor:
        """A high-speed compressor using the zstandard library."""

        def __init__(self, level: int = 3) -> None:
            """
            Initializes the compressor with a specific compression level.

            The level can be between 1 (fastest) and 22 (best compression).
            If not specified, it defaults to 3, which is a good balance.
            """
            self.level = level

        def compress(self, data: bytes) -> bytes:
            """Compresses the input bytes using zstandard and returns compressed bytes."""
            return zstandard.compress(data, level=self.level)

        def decompress(self, data: bytes) -> bytes:
            """Decompresses the input bytes using zstandard and returns original bytes."""
            return zstandard.decompress(data)

except ImportError:
    # If zstandard is not installed, this class will not be defined.
    pass


# LzoCompressor is only available if the 'lzo' library is installed.
try:
    import lzo  # type: ignore

    class LzoCompressor:
        """A very fast compressor using the python-lzo library."""

        def __init__(self, level: int = 1) -> None:
            """
            Initializes the compressor with a specific compression level.

            The level can be between 1 (fastest) and 9 (best compression).
            If not specified, it defaults to 1, which is the fastest.
            """
            self.level = level

        def compress(self, data: bytes) -> bytes:
            """Compresses the input bytes using lzo and returns compressed bytes."""
            return lzo.compress(data, self.level)

        def decompress(self, data: bytes) -> bytes:
            """Decompresses the input bytes using lzo and returns original bytes."""
            return lzo.decompress(data)

except ImportError:
    # If lzo is not installed, this class will not be defined.
    pass
