"""
Core implementation of the synchronous and asynchronous compressed queues.
"""

import collections
import threading

from .compressors import (
    Compressor,
    Serializer,
)

from .base import QItem, _QueueElement, _BaseCompressedQueue


# --- 1. The Synchronous Deque Implementation ---
class CompressedDeque(
    _BaseCompressedQueue[QItem, "collections.deque[_QueueElement]"]
):
    """
    A thread-safe, synchronous double-ended queue (deque) that transparently
    compresses any picklable object using pluggable compression and serialization
    strategies. It supports operations from both ends and is suitable for
    scenarios requiring FIFO, LIFO, or mixed access patterns.
    """

    def __init__(
        self,
        maxsize: int | None = None,
        *,
        compressor: Compressor | None = None,
        serializer: Serializer | None = None,
    ):
        """
        Initializes a new instance of CompressedDeque with the specified parameters.

        This constructor initializes a new instance of CompressedDeque with the provided
        parameters, creating an underlying deque instance with the specified maxsize.
        It also allows overriding the default compressor and serializer, otherwise using
        the ZlibCompressor and PickleSerializer.

        Additionally, a lock is created to ensure thread-safe operations, as deque operations
        are atomic but the class may require additional synchronization for thread safety.

        Args:
            maxsize (int | None, optional): The maximum size of the deque. Defaults to None (unbounded).
            compressor (Compressor, optional): The compressor to use. Defaults to None (ZlibCompressor).
            serializer (Serializer, optional): The serializer to use. Defaults to None (PickleSerializer).
        """
        # Use collections.deque with maxlen for bounded size.
        super().__init__(
            collections.deque(maxlen=maxsize), compressor, serializer
        )
        # Additional lock for deque operations, as deque is atomic but we need
        # to ensure consistency with stats and multi-step ops.
        self._lock = threading.Lock()

    def qsize(self) -> int:
        """
        Return the current size of the deque.

        This method returns the current number of items in the deque. It ensures thread-safe
        access by acquiring a lock before returning the size of the underlying deque.

        Returns:
            int: The number of items in the deque.
        """
        with self._lock:
            return len(self._queue)

    def empty(self) -> bool:
        """
        Return True if the deque is empty, False otherwise.

        This method returns a boolean indicating whether the deque currently contains any
        items. It ensures thread-safe access by acquiring a lock before checking the
        length of the underlying deque.

        Returns:
            bool: True if the deque is empty, False otherwise.
        """
        with self._lock:
            return len(self._queue) == 0

    def full(self) -> bool:
        """Return True if the deque is full (reached maxlen), False otherwise."""
        with self._lock:
            return (
                self._queue.maxlen is not None
                and len(self._queue) == self._queue.maxlen
            )

    def append(self, item: QItem) -> None:
        """
        Serialize, compress, and append an item to the right end of the deque.

        Args:
            item (QItem): The item to serialize, compress, and append to the deque.
        """
        raw_bytes = self.serializer.dumps(item)
        compressed_bytes = self.compressor.compress(raw_bytes)

        element = _QueueElement(
            compressed_data=compressed_bytes, raw_size=len(raw_bytes)
        )

        # Lock for stats update
        with self._stats_lock:
            self._total_raw_size += element.raw_size
            self._total_compressed_size += len(element.compressed_data)

        # Lock for deque operation
        with self._lock:
            evicted = None
            if (
                self._queue.maxlen is not None
                and len(self._queue) == self._queue.maxlen
            ):
                evicted = self._queue[0]  # Leftmost item will be evicted
            self._queue.append(element)
            if evicted:
                self._total_raw_size -= evicted.raw_size
                self._total_compressed_size -= len(evicted.compressed_data)

    def appendleft(self, item: QItem) -> None:
        """
        Serialize, compress, and append an item to the left end of the deque.

        Args:
            item (QItem): The item to serialize, compress, and append to left end of the deque.
        """
        raw_bytes = self.serializer.dumps(item)
        compressed_bytes = self.compressor.compress(raw_bytes)

        element = _QueueElement(
            compressed_data=compressed_bytes, raw_size=len(raw_bytes)
        )

        # Lock for stats update.
        with self._stats_lock:
            self._total_raw_size += element.raw_size
            self._total_compressed_size += len(element.compressed_data)

        # Lock for deque operation.
        with self._lock:
            evicted = None
            if (
                self._queue.maxlen is not None
                and len(self._queue) == self._queue.maxlen
            ):
                evicted = self._queue[-1]  # Rightmost item will be evicted
            self._queue.appendleft(element)
            if evicted:
                self._total_raw_size -= evicted.raw_size
                self._total_compressed_size -= len(evicted.compressed_data)

    def pop(self) -> QItem:
        """
        Pop an item from the right end of the deque, decompress, deserialize, and return it.

        Returns:
            QItem: The deserialized and decompressed item retrieved from the right end of the deque.
        """
        # Lock for deque operation.
        with self._lock:
            element = self._queue.pop()

        # Lock for stats update.
        with self._stats_lock:
            self._total_raw_size -= element.raw_size
            self._total_compressed_size -= len(element.compressed_data)

        raw_bytes = self.compressor.decompress(element.compressed_data)
        item = self.serializer.loads(raw_bytes)

        return item

    def popleft(self) -> QItem:
        """
        Pop an item from the left end of the deque, decompress, deserialize, and return it.

        Returns:
            QItem: The deserialized and decompressed item retrieved from the left end of the deque.
        """
        # Lock for deque operation.
        with self._lock:
            element = self._queue.popleft()

        # Lock for stats update.
        with self._stats_lock:
            self._total_raw_size -= element.raw_size
            self._total_compressed_size -= len(element.compressed_data)

        raw_bytes = self.compressor.decompress(element.compressed_data)
        item = self.serializer.loads(raw_bytes)

        return item


# --- 2. The Asynchronous Deque Implementation ---
class AsyncCompressedDeque(
    _BaseCompressedQueue[QItem, "collections.deque[_QueueElement]"]
):
    """
    An asyncio-compatible double-ended queue (deque) that transparently compresses
    any picklable object using pluggable compression and serialization strategies.
    It supports operations from both ends and is suitable for async scenarios
    requiring FIFO, LIFO, or mixed access patterns.
    """

    def __init__(
        self,
        maxsize: int | None = None,
        *,
        compressor: Compressor | None = None,
        serializer: Serializer | None = None,
    ):
        """
        Initializes a new instance of AsyncCompressedDeque with the specified parameters.

        This constructor initializes a new instance of AsyncCompressedDeque with a
        collections.deque instance, using the specified maxsize for bounded size.
        It also allows overriding the default compressor and serializer, otherwise using
        the ZlibCompressor and PickleSerializer.

        Args:
            maxsize (int | None, optional): The maximum size of the deque. Defaults to None (unbounded).
            compressor (Compressor, optional): The compressor to use. Defaults to None (ZlibCompressor).
            serializer (Serializer, optional): The serializer to use. Defaults to None (PickleSerializer).

        Raises:
            RuntimeError: If initialized outside a running asyncio event loop.
        """
        super().__init__(
            collections.deque(maxlen=maxsize), compressor, serializer
        )
        self._lock = threading.Lock()
        self._loop = self._get_running_loop()

    def _serialize_and_compress(self, item: QItem) -> _QueueElement:
        """Synchronously serialize and compress an item."""
        raw_bytes = self.serializer.dumps(item)
        compressed_bytes = self.compressor.compress(raw_bytes)
        return _QueueElement(
            compressed_data=compressed_bytes, raw_size=len(raw_bytes)
        )

    def _decompress_and_deserialize(self, element: _QueueElement) -> QItem:
        """Synchronously decompress and deserialize an item."""
        raw_bytes = self.compressor.decompress(element.compressed_data)
        return self.serializer.loads(raw_bytes)

    def qsize(self) -> int:
        """
        Return the current size of the deque.

        This method returns the current number of items in the deque. It ensures
        asyncio-safe access by acquiring a lock before checking the length.

        Returns:
            int: The number of items in the deque.
        """
        with self._lock:
            return len(self._queue)

    def empty(self) -> bool:
        """
        Return True if the deque is empty, False otherwise.

        This method checks if the deque is empty, ensuring asyncio-safe access by
        acquiring a lock before checking the length.

        Returns:
            bool: True if the deque is empty, False otherwise.
        """
        with self._lock:
            return len(self._queue) == 0

    def full(self) -> bool:
        """
        Return True if the deque is full (reached maxlen), False otherwise.

        This method checks if the deque has reached its maximum size, ensuring
        asyncio-safe access by acquiring a lock.

        Returns:
            bool: True if the deque is full, False otherwise.
        """
        with self._lock:
            return (
                self._queue.maxlen is not None
                and len(self._queue) == self._queue.maxlen
            )

    async def append(self, item: QItem) -> None:
        """
        Serialize, compress, and append an item to the right end of the deque.

        If the deque is full (i.e., its length equals maxlen), this method removes
        the leftmost item before appending the new item, and updates the statistics
        to reflect the evicted item.

        Args:
            item (QItem): The item to serialize, compress, and append to the deque.
        """
        loop = self._loop
        assert loop is not None, "Event loop not available"

        element = await loop.run_in_executor(
            None, self._serialize_and_compress, item
        )

        with self._stats_lock:
            self._total_raw_size += element.raw_size
            self._total_compressed_size += len(element.compressed_data)

        with self._lock:
            evicted = None
            if (
                self._queue.maxlen is not None
                and len(self._queue) == self._queue.maxlen
            ):
                evicted = self._queue[0]  # Leftmost item will be evicted
            self._queue.append(element)
            if evicted:
                with self._stats_lock:
                    self._total_raw_size -= evicted.raw_size
                    self._total_compressed_size -= len(evicted.compressed_data)

    def append_nowait(self, item: QItem) -> None:
        """
        Serialize, compress, and append an item to the right end of the deque
        without blocking. This is a synchronous operation.

        Args:
            item (QItem): The item to serialize, compress, and append.
        """
        element = self._serialize_and_compress(item)

        with self._stats_lock:
            self._total_raw_size += element.raw_size
            self._total_compressed_size += len(element.compressed_data)

        with self._lock:
            evicted = None
            if (
                self._queue.maxlen is not None
                and len(self._queue) == self._queue.maxlen
            ):
                evicted = self._queue[0]
            self._queue.append(element)
            if evicted:
                with self._stats_lock:
                    self._total_raw_size -= evicted.raw_size
                    self._total_compressed_size -= len(evicted.compressed_data)

    async def appendleft(self, item: QItem) -> None:
        """
        Serialize, compress, and append an item to the left end of the deque.

        If the deque is full (i.e., its length equals maxlen), this method removes
        the rightmost item before appending the new item, and updates the statistics
        to reflect the evicted item.

        Args:
            item (QItem): The item to serialize, compress, and append to the left end of the deque.
        """
        loop = self._loop
        assert loop is not None, "Event loop not available"

        element = await loop.run_in_executor(
            None, self._serialize_and_compress, item
        )

        with self._stats_lock:
            self._total_raw_size += element.raw_size
            self._total_compressed_size += len(element.compressed_data)

        with self._lock:
            evicted = None
            if (
                self._queue.maxlen is not None
                and len(self._queue) == self._queue.maxlen
            ):
                evicted = self._queue[-1]  # Rightmost item will be evicted
            self._queue.appendleft(element)
            if evicted:
                with self._stats_lock:
                    self._total_raw_size -= evicted.raw_size
                    self._total_compressed_size -= len(evicted.compressed_data)

    def appendleft_nowait(self, item: QItem) -> None:
        """
        Serialize, compress, and append an item to the left end of the deque
        without blocking. This is a synchronous operation.

        Args:
            item (QItem): The item to serialize, compress, and append.
        """
        element = self._serialize_and_compress(item)

        with self._stats_lock:
            self._total_raw_size += element.raw_size
            self._total_compressed_size += len(element.compressed_data)

        with self._lock:
            evicted = None
            if (
                self._queue.maxlen is not None
                and len(self._queue) == self._queue.maxlen
            ):
                evicted = self._queue[-1]
            self._queue.appendleft(element)
            if evicted:
                with self._stats_lock:
                    self._total_raw_size -= evicted.raw_size
                    self._total_compressed_size -= len(evicted.compressed_data)

    async def pop(self) -> QItem:
        """
        Pop an item from the right end of the deque, decompress, deserialize, and return it.

        Returns:
            QItem: The deserialized and decompressed item retrieved from the right end of the deque.

        Raises:
            IndexError: If the deque is empty.
        """
        loop = self._loop
        assert loop is not None, "Event loop not available"

        with self._lock:
            if len(self._queue) == 0:
                raise IndexError("pop from an empty deque")
            element = self._queue.pop()

        with self._stats_lock:
            self._total_raw_size -= element.raw_size
            self._total_compressed_size -= len(element.compressed_data)

        item = await loop.run_in_executor(
            None, self._decompress_and_deserialize, element
        )
        return item

    def pop_nowait(self) -> QItem:
        """
        Pop an item from the right end of the deque, decompress, and
        deserialize it without blocking. This is a synchronous operation.

        Returns:
            QItem: The deserialized and decompressed item.

        Raises:
            IndexError: If the deque is empty.
        """
        with self._lock:
            if len(self._queue) == 0:
                raise IndexError("pop from an empty deque")
            element = self._queue.pop()

        with self._stats_lock:
            self._total_raw_size -= element.raw_size
            self._total_compressed_size -= len(element.compressed_data)

        item = self._decompress_and_deserialize(element)
        return item

    async def popleft(self) -> QItem:
        """
        Pop an item from the left end of the deque, decompress, deserialize, and return it.

        Returns:
            QItem: The deserialized and decompressed item retrieved from the left end of the deque.

        Raises:
            IndexError: If the deque is empty.
        """
        loop = self._loop
        assert loop is not None, "Event loop not available"

        with self._lock:
            if len(self._queue) == 0:
                raise IndexError("pop from an empty deque")
            element = self._queue.popleft()

        with self._stats_lock:
            self._total_raw_size -= element.raw_size
            self._total_compressed_size -= len(element.compressed_data)

        item = await loop.run_in_executor(
            None, self._decompress_and_deserialize, element
        )
        return item

    def popleft_nowait(self) -> QItem:
        """
        Pop an item from the left end of the deque, decompress, and
        deserialize it without blocking. This is a synchronous operation.

        Returns:
            QItem: The deserialized and decompressed item.

        Raises:
            IndexError: If the deque is empty.
        """
        with self._lock:
            if len(self._queue) == 0:
                raise IndexError("pop from an empty deque")
            element = self._queue.popleft()

        with self._stats_lock:
            self._total_raw_size -= element.raw_size
            self._total_compressed_size -= len(element.compressed_data)

        item = self._decompress_and_deserialize(element)
        return item
