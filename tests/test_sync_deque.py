"""
Tests for the synchronous, thread-safe CompressedDeque.
"""

import random
import string
import threading
import time

import pytest

from quez import CompressedDeque
from quez.compressors import (
    Bz2Compressor,
    Compressor,
    LzmaCompressor,
    NullCompressor,
    ZlibCompressor,
)

# --- Test Fixtures and Parameters ---
COMPRESSOR_FIXTURES = [
    ZlibCompressor(),
    Bz2Compressor(),
    LzmaCompressor(),
    NullCompressor(),
]

TEST_ITEMS = [
    "a_string",
    b"some_bytes",
    {"a_dict": 123},
    ["a", "list", 1],
]


@pytest.fixture(params=COMPRESSOR_FIXTURES)
def compressor(request) -> Compressor:
    """Provides each compressor implementation for parametrization."""
    return request.param


# --- Core Functionality Tests ---
def test_append_pop_roundtrip(compressor: Compressor):
    """Test basic append and pop operations for a single item."""
    q: CompressedDeque = CompressedDeque(compressor=compressor)
    item = {"data": "test_data"}
    q.append(item)
    retrieved_item = q.pop()
    assert retrieved_item == item


def test_appendleft_popleft_roundtrip(compressor: Compressor):
    """Test basic appendleft and popleft operations for a single item."""
    q: CompressedDeque = CompressedDeque(compressor=compressor)
    item = {"data": "test_data"}
    q.appendleft(item)
    retrieved_item = q.popleft()
    assert retrieved_item == item


def test_fifo_ordering(compressor: Compressor):
    """Test that items are retrieved in FIFO order using append and popleft."""
    q: CompressedDeque = CompressedDeque(compressor=compressor)
    items = [f"item-{i}" for i in range(5)]
    for item in items:
        q.append(item)

    retrieved_items = [q.popleft() for _ in items]
    assert retrieved_items == items


def test_lifo_ordering(compressor: Compressor):
    """Test that items are retrieved in LIFO order using append and pop."""
    q: CompressedDeque = CompressedDeque(compressor=compressor)
    items = [f"item-{i}" for i in range(5)]
    for item in items:
        q.append(item)

    retrieved_items = [q.pop() for _ in items]
    assert retrieved_items == items[::-1]


@pytest.mark.parametrize("item", TEST_ITEMS)
def test_various_data_types(compressor: Compressor, item):
    """Test that the deque correctly handles various data types."""
    q: CompressedDeque = CompressedDeque(compressor=compressor)
    q.append(item)
    assert q.pop() == item


# --- Deque Property Tests ---
def test_qsize_empty_full(compressor: Compressor):
    """Test qsize(), empty(), and full() methods."""
    q: CompressedDeque = CompressedDeque(maxsize=2, compressor=compressor)
    assert q.qsize() == 0
    assert q.empty()
    assert not q.full()

    q.append("item1")
    assert q.qsize() == 1
    assert not q.empty()
    assert not q.full()

    q.append("item2")
    assert q.qsize() == 2
    assert not q.empty()
    assert q.full()

    q.pop()
    assert q.qsize() == 1
    assert not q.full()


# --- Stats Tests ---
def test_stats_calculation(compressor: Compressor):
    """Test that statistics are calculated correctly."""
    q: CompressedDeque = CompressedDeque(compressor=compressor)
    item = "test_item_for_stats" * 10

    # Check initial state
    stats = q.stats
    assert stats["count"] == 0
    assert stats["raw_size_bytes"] == 0
    assert stats["compressed_size_bytes"] == 0
    assert stats["compression_ratio_pct"] is None

    q.append(item)
    stats = q.stats
    assert stats["count"] == 1
    assert stats["raw_size_bytes"] > 0
    assert stats["compressed_size_bytes"] > 0

    if not isinstance(compressor, NullCompressor):
        assert stats["raw_size_bytes"] > stats["compressed_size_bytes"]
        assert 0 < stats["compression_ratio_pct"] < 100
    else:
        assert stats["raw_size_bytes"] == stats["compressed_size_bytes"]
        assert stats["compression_ratio_pct"] == 0.0

    q.pop()
    stats = q.stats
    assert stats["count"] == 0
    assert stats["raw_size_bytes"] == 0
    assert stats["compressed_size_bytes"] == 0


# --- Concurrency and Threading Tests ---
def test_thread_safety(compressor: Compressor):
    """Test that the deque is thread-safe for appends, pops, and stats."""
    q: CompressedDeque = CompressedDeque(compressor=compressor)
    num_items = 100
    items_to_append = list(range(num_items))
    retrieved_items: list = []

    def producer():
        for item in items_to_append:
            q.append(item)
            time.sleep(0.001)

    def consumer():
        while len(retrieved_items) < num_items:
            try:
                item = q.popleft()
                retrieved_items.append(item)
            except IndexError:
                time.sleep(0.001)  # Spin wait if empty

    producer_thread = threading.Thread(target=producer)
    consumer_thread = threading.Thread(target=consumer)

    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()

    assert q.empty()
    assert len(retrieved_items) == num_items
    assert sorted(retrieved_items) == items_to_append

    stats = q.stats
    assert stats["count"] == 0
    assert stats["raw_size_bytes"] == 0
    assert stats["compressed_size_bytes"] == 0


# --- Eviction Tests ---
def test_eviction_stats(compressor: Compressor):
    """Test that stats are updated correctly when items are evicted."""
    q: CompressedDeque = CompressedDeque(maxsize=2, compressor=compressor)

    # Generate random strings of varying lengths (1000–2000 characters) for each item.
    # This ensures that the compressed sizes will differ, which is necessary to validate
    # the eviction-aware logic under test.
    item1 = "".join(random.choices(string.ascii_lowercase, k=1000))
    item2 = "".join(random.choices(string.ascii_lowercase, k=1500))
    item3 = "".join(random.choices(string.ascii_lowercase, k=2000))

    # Compute expected sizes for each item
    from quez.compressors import PickleSerializer

    serializer = PickleSerializer()
    raw1 = len(serializer.dumps(item1))
    compressed1 = len(compressor.compress(serializer.dumps(item1)))
    raw2 = len(serializer.dumps(item2))
    compressed2 = len(compressor.compress(serializer.dumps(item2)))
    raw3 = len(serializer.dumps(item3))
    compressed3 = len(compressor.compress(serializer.dumps(item3)))

    # Verify that the compressed sizes are distinct (when using a real compressor).
    # This check ensures the test setup is valid and that compression behaves as expected.
    if not isinstance(compressor, NullCompressor):
        assert (
            len({compressed1, compressed2, compressed3}) == 3
        ), f"Compressed sizes must be different, got {compressed1}, {compressed2}, {compressed3}"

    q.append(item1)  # Add first item
    stats = q.stats
    assert stats["count"] == 1
    assert stats["raw_size_bytes"] == raw1
    assert stats["compressed_size_bytes"] == compressed1

    q.append(item2)  # Add second item
    stats = q.stats
    assert stats["count"] == 2
    assert stats["raw_size_bytes"] == raw1 + raw2
    assert stats["compressed_size_bytes"] == compressed1 + compressed2

    q.append(item3)  # Evict item1, add item3
    stats = q.stats
    assert stats["count"] == 2
    assert stats["raw_size_bytes"] == raw2 + raw3
    assert stats["compressed_size_bytes"] == compressed2 + compressed3

    assert q.popleft() == item2  # FIFO order
    assert q.popleft() == item3
    assert q.empty()


def test_appendleft_eviction_stats(compressor: Compressor):
    """Test eviction when appendleft pushes out the rightmost item."""
    q: CompressedDeque = CompressedDeque(maxsize=2, compressor=compressor)

    item1 = "".join(random.choices(string.ascii_lowercase, k=1000))
    item2 = "".join(random.choices(string.ascii_lowercase, k=1500))
    item3 = "".join(random.choices(string.ascii_lowercase, k=2000))

    from quez.compressors import PickleSerializer

    serializer = PickleSerializer()

    raw1 = len(serializer.dumps(item1))
    raw2 = len(serializer.dumps(item2))
    raw3 = len(serializer.dumps(item3))
    compressed1 = len(compressor.compress(serializer.dumps(item1)))
    compressed2 = len(compressor.compress(serializer.dumps(item2)))
    compressed3 = len(compressor.compress(serializer.dumps(item3)))

    if not isinstance(compressor, NullCompressor):
        assert len({compressed1, compressed2, compressed3}) == 3

    q.append(item1)
    q.append(item2)
    stats = q.stats
    assert stats["count"] == 2
    assert stats["raw_size_bytes"] == raw1 + raw2

    q.appendleft(item3)  # Evicts item2
    stats = q.stats
    assert stats["count"] == 2
    assert stats["raw_size_bytes"] == raw1 + raw3

    assert q.pop() == item1
    assert q.pop() == item3
    assert q.empty()


def test_pop_popleft_empty(compressor: Compressor):
    """Ensure popping from an empty CompressedDeque raises IndexError."""
    q: CompressedDeque = CompressedDeque(compressor=compressor)

    with pytest.raises(IndexError, match="pop from an empty deque"):
        q.pop()

    with pytest.raises(IndexError, match="pop from an empty deque"):
        q.popleft()
