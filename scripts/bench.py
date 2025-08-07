"""
Quez Benchmark Script

This script measures the performance of all data structures (`Queue`, `Deque`)
in both synchronous and asynchronous modes across all available compressors.

It measures:
- Throughput (items/sec) for putting/appending items.
- Throughput (items/sec) for getting/popping items.
- Final compressed size and compression ratio.

Usage:
    poetry run python scripts/bench.py [-n NUM_ITEMS] [-s DATA_SIZE]

Example:
    poetry run python scripts/bench.py --num-items 10000 --data-size 512
"""

import argparse
import asyncio
import random
import string
import time
from typing import Any, Dict, List, Type

# --- Quez Imports ---
from quez import (
    AsyncCompressedDeque,
    AsyncCompressedQueue,
    Bz2Compressor,
    CompressedDeque,
    CompressedQueue,
    Compressor,
    LzmaCompressor,
    NullCompressor,
    ZlibCompressor,
)

# --- Helper Functions ---


def generate_sample_data(
    num_items: int, data_size: int
) -> List[Dict[str, Any]]:
    """
    Generates a list of semi-realistic, compressible JSON-like objects.
    Instead of purely random data, it repeats a smaller chunk of random
    data to simulate patterns found in real-world payloads.
    """
    print(
        f"Generating {num_items:,} sample data items (each ~{data_size} bytes)..."
    )
    samples = []

    # Create a repeatable chunk to make data compressible
    chunk_size = 256 if data_size > 512 else data_size // 4
    base_chunk = "".join(
        random.choices(string.ascii_letters + string.digits, k=chunk_size)
    )

    for i in range(num_items):
        # Repeat the chunk to build the payload
        repeats = (data_size // chunk_size) + 1
        payload = (base_chunk * repeats)[:data_size]

        item = {
            "id": i,
            "timestamp": time.time(),
            "payload": payload,
        }
        samples.append(item)

    print("Sample data generated.\n")
    return samples


def get_available_compressors() -> List[Compressor]:
    """
    Returns a list of all available compressors, including optional ones
    if they are installed.
    """
    compressors = [
        NullCompressor(),
        ZlibCompressor(),
        Bz2Compressor(),
        LzmaCompressor(),
    ]
    try:
        from quez.compressors import ZstdCompressor

        compressors.append(ZstdCompressor())
    except ImportError:
        pass
    try:
        from quez.compressors import LzoCompressor

        compressors.append(LzoCompressor())
    except ImportError:
        pass
    return compressors  # noqa


def format_size(size_bytes: int) -> str:
    """Formats bytes into a human-readable string (KB, MB)."""
    if size_bytes > 1024 * 1024:
        return f"{size_bytes / (1024*1024):.2f} MB"
    return f"{size_bytes / 1024:.2f} KB"


# --- Benchmark Runners ---


def run_sync_benchmark(
    queue_class: Type[CompressedQueue | CompressedDeque],
    compressor: Compressor,
    data: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Runs a benchmark for a synchronous Quez data structure."""
    q = queue_class(compressor=compressor)

    # --- Benchmark Putting/Appending ---
    put_start_time = time.perf_counter()
    if isinstance(q, CompressedQueue):
        for item in data:
            q.put(item)
    else:  # CompressedDeque
        for item in data:
            q.append(item)
    put_end_time = time.perf_counter()

    final_stats = q.stats

    # --- Benchmark Getting/Popping ---
    get_start_time = time.perf_counter()
    if isinstance(q, CompressedQueue):
        for _ in range(len(data)):
            q.get()
    else:  # CompressedDeque
        for _ in range(len(data)):
            q.popleft()
    get_end_time = time.perf_counter()

    put_duration = put_end_time - put_start_time
    get_duration = get_end_time - get_start_time

    return {
        "put_throughput": len(data) / put_duration,
        "get_throughput": len(data) / get_duration,
        "stats": final_stats,
    }


async def run_async_benchmark(
    queue_class: Type[AsyncCompressedQueue | AsyncCompressedDeque],
    compressor: Compressor,
    data: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Runs a benchmark for an asynchronous Quez data structure."""
    q = queue_class(compressor=compressor)

    # --- Benchmark Putting/Appending ---
    put_start_time = time.perf_counter()
    if isinstance(q, AsyncCompressedQueue):
        await asyncio.gather(*(q.put(item) for item in data))
    else:  # AsyncCompressedDeque
        await asyncio.gather(*(q.append(item) for item in data))
    put_end_time = time.perf_counter()

    final_stats = q.stats

    # --- Benchmark Getting/Popping ---
    get_start_time = time.perf_counter()
    if isinstance(q, AsyncCompressedQueue):
        _ = await asyncio.gather(*(q.get() for _ in range(len(data))))
    else:  # AsyncCompressedDeque
        _ = await asyncio.gather(*(q.popleft() for _ in range(len(data))))
    get_end_time = time.perf_counter()

    put_duration = put_end_time - put_start_time
    get_duration = get_end_time - get_start_time

    return {
        "put_throughput": len(data) / put_duration,
        "get_throughput": len(data) / get_duration,
        "stats": final_stats,
    }


def run_nowait_benchmark(
    queue_class: Type[AsyncCompressedQueue | AsyncCompressedDeque],
    compressor: Compressor,
    data: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Runs a benchmark for a Quez data structure using _nowait methods."""
    # For async classes, we need to initialize them within a running loop.
    try:
        loop = asyncio.get_running_loop()
        q = queue_class(compressor=compressor)
    except RuntimeError:
        # If no loop is running, create one temporarily for initialization.
        q = asyncio.run(_async_init_helper(queue_class, compressor))

    # --- Benchmark Putting/Appending ---
    put_start_time = time.perf_counter()
    if isinstance(q, (AsyncCompressedQueue)):
        for item in data:
            q.put_nowait(item)
    else:  # AsyncCompressedDeque
        for item in data:
            q.append_nowait(item)
    put_end_time = time.perf_counter()

    final_stats = q.stats

    # --- Benchmark Getting/Popping ---
    get_start_time = time.perf_counter()
    if isinstance(q, (AsyncCompressedQueue)):
        for _ in range(len(data)):
            q.get_nowait()
    else:  # AsyncCompressedDeque
        for _ in range(len(data)):
            q.popleft_nowait()
    get_end_time = time.perf_counter()

    put_duration = put_end_time - put_start_time
    get_duration = get_end_time - get_start_time

    return {
        "put_throughput": len(data) / put_duration,
        "get_throughput": len(data) / get_duration,
        "stats": final_stats,
    }


async def _async_init_helper(queue_class, compressor):
    """Helper to initialize async queues in a controlled event loop."""
    return queue_class(compressor=compressor)


# --- Main Execution ---


def main(num_items: int, data_size: int):
    """Main function to run all benchmarks and print results."""
    print("--- Quez Benchmark Suite ---")
    print(f"Items per test: {num_items:,}")
    print(f"Data size per item: {data_size:,} bytes")

    sample_data = generate_sample_data(num_items, data_size)
    compressors = get_available_compressors()
    results = []

    # --- Run All Benchmarks ---
    for compressor in compressors:
        comp_name = compressor.__class__.__name__

        # Sync Queue
        sync_q_result = run_sync_benchmark(
            CompressedQueue, compressor, sample_data
        )
        results.append(("CompressedQueue", comp_name, sync_q_result))

        # Sync Deque
        sync_d_result = run_sync_benchmark(
            CompressedDeque, compressor, sample_data
        )
        results.append(("CompressedDeque", comp_name, sync_d_result))

        # Async Queue
        async_q_result = asyncio.run(
            run_async_benchmark(AsyncCompressedQueue, compressor, sample_data)
        )
        results.append(("AsyncCompressedQueue", comp_name, async_q_result))

        # Async Deque
        async_d_result = asyncio.run(
            run_async_benchmark(AsyncCompressedDeque, compressor, sample_data)
        )
        results.append(("AsyncCompressedDeque", comp_name, async_d_result))

        # Async Queue (nowait)
        async_q_nowait_result = run_nowait_benchmark(
            AsyncCompressedQueue, compressor, sample_data
        )
        results.append(
            ("AsyncCompressedQueue (nowait)", comp_name, async_q_nowait_result)
        )

        # Async Deque (nowait)
        async_d_nowait_result = run_nowait_benchmark(
            AsyncCompressedDeque, compressor, sample_data
        )
        results.append(
            ("AsyncCompressedDeque (nowait)", comp_name, async_d_nowait_result)
        )

    # --- Print Results Table ---
    print("\n--- Benchmark Results ---\n")
    header = (
        f"{'Data Structure':<30} | {'Compressor':<18} | {'Put Throughput':>18} | "
        f"{'Get Throughput':>18} | {'Compressed Size':>18} | {'Ratio':>8}"
    )
    print(header)
    print("-" * len(header))

    for name, comp, result in results:
        stats = result["stats"]
        put_t = f"{result['put_throughput']:,.0f} items/s"
        get_t = f"{result['get_throughput']:,.0f} items/s"
        size = format_size(stats["compressed_size_bytes"])
        ratio = (
            f"{stats['compression_ratio_pct']:.2f}%"
            if stats["compression_ratio_pct"] is not None
            else "N/A"
        )

        print(
            f"{name:<30} | {comp:<18} | {put_t:>18} | "
            f"{get_t:>18} | {size:>18} | {ratio:>8}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run benchmark suite for the Quez library.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-n",
        "--num-items",
        type=int,
        default=5_000,
        help="Number of items to process in each benchmark run.",
    )
    parser.add_argument(
        "-s",
        "--data-size",
        type=int,
        default=1024,
        help="Size of the data payload for each item, in bytes.",
    )
    args = parser.parse_args()

    try:
        import uvloop
    except ImportError:
        pass
    else:
        uvloop.install()  # Use uvloop for better performance with asyncio

    main(num_items=args.num_items, data_size=args.data_size)
