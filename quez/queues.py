# qzq/queues.py
"""
Core implementation of the synchronous and asynchronous compressed queues.
"""
import asyncio
import queue
import threading
from typing import Tuple, TypeVar, Generic, Optional, Union, Dict, Any
from datetime import datetime

from .compressors import Compressor, ZlibCompressor

# --- Generic Type Variables ---
QItem = TypeVar("QItem")
QueueType = TypeVar("QueueType", bound=Union[queue.Queue, asyncio.Queue])


# --- 1. The Shared Base Class ---
class _BaseCompressedQueue(Generic[QItem, QueueType]):
    """
    A generic base class holding the common logic for compressed queues.
    This class should not be instantiated directly.
    """
    def __init__(self,
                 queue_instance: QueueType,
                 compressor: Optional[Compressor] = None):
        """
        Initializes the base queue.

        Args:
            queue_instance: An instance of a sync or async queue.
            compressor: A pluggable compressor object. Defaults to ZlibCompressor.
        """
        self._queue: QueueType = queue_instance
        self.compressor = compressor if compressor is not None else ZlibCompressor()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        
        self._stats_lock = threading.Lock()
        self._total_raw_size: int = 0
        self._total_compressed_size: int = 0

    def _get_running_loop(self) -> asyncio.AbstractEventLoop:
        """Shared helper to get the running event loop for the async version."""
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        return self._loop

    @property
    def stats(self) -> Dict[str, Any]:
        """
        Returns a dictionary with statistics about the items currently in the queue.
        The compression ratio will be None if the queue is empty.
        """
        with self._stats_lock:
            n = self.qsize()
            raw = self._total_raw_size
            compressed = self._total_compressed_size
            
            # The ratio is None if raw size is zero (i.e., queue is empty)
            ratio = (1 - (compressed / raw)) * 100 if raw > 0 else None

            return {
                'n': n,
                'raw': raw,
                'compressed': compressed,
                'ratio': ratio,
            }

    def qsize(self) -> int:
        """Return the approximate size of the queue."""
        return self._queue.qsize()

    def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise."""
        return self._queue.empty()

    def full(self) -> bool:
        """Return True if the queue is full, False otherwise."""
        return self._queue.full()


# --- 2. The Asynchronous Implementation ---
class AsyncCompressedQueue(_BaseCompressedQueue[QItem, asyncio.Queue]):
    """
    An asyncio-compatible queue that transparently compresses data
    using a pluggable compression strategy.
    """
    def __init__(self, maxsize: int = 0, *, compressor: Optional[Compressor] = None):
        super().__init__(asyncio.Queue(maxsize), compressor)

    async def put(self, item: QItem):
        """Compresses data and puts it onto the queue in a non-blocking way."""
        raw_bytes, *metadata = item
        loop = self._get_running_loop()
        compressed_bytes = await loop.run_in_executor(
            None, self.compressor.compress, raw_bytes
        )
        
        # Asyncio operations are single-threaded, so no lock is needed here
        self._total_raw_size += len(raw_bytes)
        self._total_compressed_size += len(compressed_bytes)
        
        await self._queue.put((compressed_bytes, *metadata))

    async def get(self) -> QItem:
        """Retrieves an item and decompresses it in a non-blocking way."""
        compressed_bytes, *metadata = await self._queue.get()
        loop = self._get_running_loop()
        raw_bytes = await loop.run_in_executor(
            None, self.compressor.decompress, compressed_bytes
        )
        
        self._total_raw_size -= len(raw_bytes)
        self._total_compressed_size -= len(compressed_bytes)

        self._queue.task_done()
        return (raw_bytes, *metadata)

    async def join(self):
        """Blocks until all items in the queue have been gotten and processed."""
        await self._queue.join()


# --- 3. The Synchronous Implementation ---
class CompressedQueue(_BaseCompressedQueue[QItem, queue.Queue]):
    """
    A thread-safe, synchronous queue that transparently compresses data
    using a pluggable compression strategy.
    """
    def __init__(self, maxsize: int = 0, *, compressor: Optional[Compressor] = None):
        super().__init__(queue.Queue(maxsize), compressor)

    def put(self, item: QItem, block: bool = True, timeout: Optional[float] = None):
        """Compresses data and puts it onto the queue, blocking if necessary."""
        raw_bytes, *metadata = item
        compressed_bytes = self.compressor.compress(raw_bytes)

        # A lock is required here for thread safety
        with self._stats_lock:
            self._total_raw_size += len(raw_bytes)
            self._total_compressed_size += len(compressed_bytes)

        self._queue.put((compressed_bytes, *metadata), block=block, timeout=timeout)

    def get(self, block: bool = True, timeout: Optional[float] = None) -> QItem:
        """Retrieves and decompresses an item, blocking if necessary."""
        compressed_bytes, *metadata = self._queue.get(block=block, timeout=timeout)
        raw_bytes = self.compressor.decompress(compressed_bytes)

        # A lock is required here for thread safety
        with self._stats_lock:
            self._total_raw_size -= len(raw_bytes)
            self._total_compressed_size -= len(compressed_bytes)

        self._queue.task_done()
        return (raw_bytes, *metadata)

    def join(self):
        """Blocks until all items in the queue have been gotten and processed."""
        self._queue.join()
