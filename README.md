# **Quez**

**Quez** is a high-performance, memory-efficient, pluggable compressed queue for buffering data in both synchronous and asynchronous Python applications.

This library excels at managing large volumes of in-memory data, making it perfect for streaming data pipelines, logging systems, or high-throughput servers. It transparently compresses objects as they enter the queue and decompresses them upon retrieval, slashing the memory footprint of in-flight data while maintaining a simple, familiar queue interface.

### Key Features

- **Dual Sync and Async Interfaces**: Offers a thread-safe `quez.CompressedQueue` for multi-threaded applications and an `quez.AsyncCompressedQueue` for `asyncio`, both with a consistent API.
- **Pluggable Compression Strategies**: Includes built-in support for zlib (default), bz2, and lzma, with optional zstd and lzo. The flexible architecture lets you plug in custom compression, serialization, or encryption algorithms.
- **Real-Time Observability**: Track queue performance with the `.stats` property, which reports item count, raw and compressed data sizes, and live compression ratio.
- **Optimized for Performance**: In the `asyncio` version, CPU-intensive compression and decompression tasks run in a background thread pool, keeping the event loop responsive.
- **Memory Efficiency**: Handles large, temporary data bursts without excessive memory usage, preventing swapping and performance degradation.