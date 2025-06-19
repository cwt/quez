# **quez**

**quez** is a pluggable compressed queue for high-performance, memory-efficient buffering in both synchronous and asynchronous Python applications.

This library provides a solution for managing large volumes of in-memory data, such as in streaming data pipelines, logging systems, or high-throughput servers. It transparently compresses objects as they enter a queue and decompresses them upon retrieval, drastically reducing the memory footprint of in-flight data while preserving a simple, familiar queue interface.

### Key Features

* **Dual Sync and Async Interfaces**: Provides both a thread-safe `quez.CompressedQueue` for multi-threaded applications and an `quez.AsyncCompressedQueue` for `asyncio`, both sharing a consistent API.
* **Pluggable Compression Strategies**: Ships with built-in, interchangeable strategies for zlib (default), bz2, and lzma. The architecture allows you to easily provide your own custom compression, serialization, or encryption algorithms.
* **Built-in Stats for Observability**: Monitor the queue's state in real-time with the `.stats` property, which provides item count, total raw and compressed data sizes, and the live compression ratio.
* **Designed for Performance**: In the `asyncio` version, CPU-bound compression and decompression tasks are automatically run in a background thread pool to keep the event loop unblocked and your application responsive.
* **Reduces Memory Pressure**: Ideal for absorbing large, temporary bursts of data without ballooning memory usage, preventing potential swapping and performance degradation.
