"""
Tests for the synchronous, thread-safe CompressedQueue.
"""

import queue
import threading
import time

import pytest

from quez import CompressedQueue
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
def test_put_get_roundtrip(compressor: Compressor):
    """Test basic put and get operations for a single item."""
    q: CompressedQueue = CompressedQueue(compressor=compressor)
    item = {"data": "test_data"}
    q.put(item)
    retrieved_item = q.get()
    assert retrieved_item == item


def test_fifo_ordering(compressor: Compressor):
    """Test that items are retrieved in First-In, First-Out order."""
    q: CompressedQueue = CompressedQueue(compressor=compressor)
    items = [f"item-{i}" for i in range(5)]
    for item in items:
        q.put(item)

    retrieved_items = [q.get() for _ in items]
    assert retrieved_items == items


@pytest.mark.parametrize("item", TEST_ITEMS)
def test_various_data_types(compressor: Compressor, item):
    """Test that the queue correctly handles various data types."""
    q: CompressedQueue = CompressedQueue(compressor=compressor)
    q.put(item)
    assert q.get() == item


# --- Queue Property Tests ---
def test_qsize_empty_full(compressor: Compressor):
    """Test qsize(), empty(), and full() methods."""
    q: CompressedQueue = CompressedQueue(maxsize=2, compressor=compressor)
    assert q.qsize() == 0
    assert q.empty()
    assert not q.full()

    q.put("item1")
    assert q.qsize() == 1
    assert not q.empty()
    assert not q.full()

    q.put("item2")
    assert q.qsize() == 2
    assert not q.empty()
    assert q.full()

    q.get()
    assert q.qsize() == 1
    assert not q.full()


def test_maxsize_blocking(compressor: Compressor):
    """Test that put() blocks when the queue is full."""
    q: CompressedQueue = CompressedQueue(maxsize=1, compressor=compressor)
    q.put("full")

    with pytest.raises(queue.Full):
        q.put("one more", block=False)


def test_get_blocking(compressor: Compressor):
    """Test that get() blocks when the queue is empty."""
    q: CompressedQueue = CompressedQueue(compressor=compressor)

    with pytest.raises(queue.Empty):
        q.get(block=False)


# --- Stats Tests ---
def test_stats_calculation(compressor: Compressor):
    """Test that statistics are calculated correctly."""
    q: CompressedQueue = CompressedQueue(compressor=compressor)
    item = "test_item_for_stats" * 10

    # Check initial state
    stats = q.stats
    assert stats["count"] == 0
    assert stats["raw_size_bytes"] == 0
    assert stats["compressed_size_bytes"] == 0
    assert stats["compression_ratio_pct"] is None

    q.put(item)
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

    q.get()
    stats = q.stats
    assert stats["count"] == 0
    assert stats["raw_size_bytes"] == 0
    assert stats["compressed_size_bytes"] == 0


def test_stats_consistency_during_blocking_put(compressor: Compressor):
    """Test that stats remain consistent during blocking put operations.

    When a put() call blocks on a full queue, stats should not count the item
    until it actually enters the queue. This prevents the bug where stats show
    the size of items not yet in the queue.
    """
    q: CompressedQueue = CompressedQueue(maxsize=1, compressor=compressor)
    first_item = "a" * 100
    second_item = "b" * 200

    q.put(first_item)
    first_stats = q.stats
    assert first_stats["count"] == 1

    second_put_started = threading.Event()
    second_put_finished = threading.Event()

    def put_blocking():
        second_put_started.set()
        q.put(second_item)
        second_put_finished.set()

    t = threading.Thread(target=put_blocking)
    t.start()

    # Wait for the blocking thread to start
    second_put_started.wait(timeout=1.0)
    time.sleep(0.1)

    # While the second put is blocked, stats should only reflect the first item
    blocking_stats = q.stats
    assert (
        blocking_stats["count"] == 1
    ), "Count should still be 1 during blocking put"
    assert (
        blocking_stats["raw_size_bytes"] == first_stats["raw_size_bytes"]
    ), "Stats should not include second item while blocking"

    # Release the blocking put
    q.get()
    t.join(timeout=1.0)
    second_put_finished.wait(timeout=0.5)

    # Now stats should reflect the second item
    final_stats = q.stats
    assert final_stats["count"] == 1, "Queue should have 1 item"
    assert (
        final_stats["raw_size_bytes"] > first_stats["raw_size_bytes"]
    ), "Final stats should include second item's size"


# --- Concurrency and Threading Tests ---
def test_join_functionality(compressor: Compressor):
    """Test that join() waits for all tasks to be done."""
    q: CompressedQueue = CompressedQueue(maxsize=5, compressor=compressor)

    def consumer():
        time.sleep(0.01)
        while not q.empty():
            q.get()
            q.task_done()

    for i in range(5):
        q.put(f"item_{i}")

    consumer_thread = threading.Thread(target=consumer)
    consumer_thread.start()

    q.join()  # This should block until the consumer finishes
    consumer_thread.join()
    assert q.empty()


def test_thread_safety(compressor: Compressor):
    """Test that the queue is thread-safe for puts, gets, and stats."""
    q: CompressedQueue = CompressedQueue(compressor=compressor)
    num_items = 100
    items_to_put = list(range(num_items))
    retrieved_items = []

    def producer():
        for item in items_to_put:
            q.put(item)
            time.sleep(0.001)

    def consumer():
        for _ in range(num_items):
            item = q.get()
            retrieved_items.append(item)
            q.task_done()
            time.sleep(0.001)

    producer_thread = threading.Thread(target=producer)
    consumer_thread = threading.Thread(target=consumer)

    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()

    assert q.empty()
    assert sorted(retrieved_items) == items_to_put

    stats = q.stats
    assert stats["count"] == 0
    assert stats["raw_size_bytes"] == 0
    assert stats["compressed_size_bytes"] == 0
