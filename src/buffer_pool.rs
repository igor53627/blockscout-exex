//! Pre-allocated buffer pools for zero-allocation I/O optimization
//!
//! This module provides lock-free buffer pooling to minimize allocation overhead
//! during MDBX database reads. Each thread maintains its own pool of 4KB buffers.

use crossbeam_queue::ArrayQueue;
use std::cell::RefCell;
use std::ops::{Deref, DerefMut};

/// Default number of buffers per thread pool
pub const DEFAULT_POOL_SIZE: usize = 1024;

/// Default buffer size (4KB - typical MDBX page size)
pub const DEFAULT_BUFFER_SIZE: usize = 4096;

/// Pre-allocated buffer pool for zero-allocation I/O
///
/// Uses a lock-free queue to minimize contention. When the pool is exhausted,
/// falls back to temporary allocation.
pub struct BufferPool {
    buffers: ArrayQueue<Vec<u8>>,
    buffer_size: usize,
    pool_size: usize,
}

impl BufferPool {
    /// Create a new buffer pool with specified capacity and buffer size
    pub fn new(pool_size: usize, buffer_size: usize) -> Self {
        let buffers = ArrayQueue::new(pool_size);

        // Pre-allocate all buffers
        for _ in 0..pool_size {
            let mut buffer = Vec::with_capacity(buffer_size);
            buffer.resize(buffer_size, 0);
            let _ = buffers.push(buffer);
        }

        Self {
            buffers,
            buffer_size,
            pool_size,
        }
    }

    /// Acquire a buffer from the pool
    ///
    /// If the pool is exhausted, allocates a temporary buffer as fallback.
    pub fn acquire(&self) -> BufferGuard<'_> {
        let buffer = self.buffers.pop().unwrap_or_else(|| {
            // Pool exhausted - allocate temporary buffer
            let mut buf = Vec::with_capacity(self.buffer_size);
            buf.resize(self.buffer_size, 0);
            buf
        });

        BufferGuard {
            buffer: Some(buffer),
            pool: self,
        }
    }

    /// Return a buffer to the pool
    ///
    /// If the pool is full, the buffer is dropped (freed).
    fn return_buffer(&self, mut buffer: Vec<u8>) {
        // Clear buffer contents for security
        buffer.clear();
        buffer.resize(self.buffer_size, 0);

        // Try to return to pool, ignore if full
        let _ = self.buffers.push(buffer);
    }

    /// Get current pool statistics
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            available: self.buffers.len(),
            capacity: self.pool_size,
            buffer_size: self.buffer_size,
        }
    }
}

/// Statistics about buffer pool usage
#[derive(Debug, Clone, Copy)]
pub struct PoolStats {
    pub available: usize,
    pub capacity: usize,
    pub buffer_size: usize,
}

/// RAII guard that automatically returns buffer to pool on drop
///
/// Implements Deref and DerefMut for transparent buffer access.
pub struct BufferGuard<'a> {
    buffer: Option<Vec<u8>>,
    pool: &'a BufferPool,
}

impl<'a> Deref for BufferGuard<'a> {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        self.buffer.as_ref().unwrap()
    }
}

impl<'a> DerefMut for BufferGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer.as_mut().unwrap()
    }
}

impl<'a> Drop for BufferGuard<'a> {
    fn drop(&mut self) {
        if let Some(buf) = self.buffer.take() {
            self.pool.return_buffer(buf);
        }
    }
}

// Thread-local buffer pool storage
thread_local! {
    static BUFFER_POOL: RefCell<BufferPool> = RefCell::new(
        BufferPool::new(DEFAULT_POOL_SIZE, DEFAULT_BUFFER_SIZE)
    );
}

/// Get a buffer from the thread-local pool
///
/// This is the primary API for acquiring pooled buffers.
pub fn get_buffer() -> BufferGuard<'static> {
    // SAFETY: We're using RefCell to ensure single-threaded access to the pool.
    // The 'static lifetime is safe because the pool lives for the lifetime of the thread.
    unsafe {
        let pool_ptr = BUFFER_POOL.with(|pool| {
            pool.as_ptr() as *const BufferPool
        });
        (*pool_ptr).acquire()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_buffer_acquire_return() {
        // RED PHASE: This test should fail initially
        let pool = BufferPool::new(10, 1024);

        // Pool should be full initially
        let stats = pool.stats();
        assert_eq!(stats.available, 10);
        assert_eq!(stats.capacity, 10);
        assert_eq!(stats.buffer_size, 1024);

        // Acquire a buffer
        {
            let buf = pool.acquire();
            assert_eq!(buf.len(), 1024);

            // Pool should have one less buffer
            let stats = pool.stats();
            assert_eq!(stats.available, 9);
        } // Buffer dropped here, should return to pool

        // Pool should be full again
        let stats = pool.stats();
        assert_eq!(stats.available, 10);
    }

    #[test]
    fn test_buffer_guard_drop() {
        // RED PHASE: Test RAII pattern
        let pool = BufferPool::new(5, 512);

        let initial_available = pool.stats().available;
        assert_eq!(initial_available, 5);

        {
            let _guard = pool.acquire();
            assert_eq!(pool.stats().available, 4);
        } // Guard dropped

        // Buffer should be returned automatically
        assert_eq!(pool.stats().available, 5);
    }

    #[test]
    fn test_pool_exhaustion() {
        // RED PHASE: Test fallback allocation when pool is exhausted
        let pool = BufferPool::new(2, 256);

        let _buf1 = pool.acquire();
        let _buf2 = pool.acquire();

        // Pool is exhausted
        assert_eq!(pool.stats().available, 0);

        // Should still work by allocating temporary buffer
        let buf3 = pool.acquire();
        assert_eq!(buf3.len(), 256);

        // Still exhausted
        assert_eq!(pool.stats().available, 0);

        // Drop first buffer
        drop(_buf1);

        // Should have one buffer back
        assert_eq!(pool.stats().available, 1);
    }

    #[test]
    fn test_thread_local_isolation() {
        // RED PHASE: Test that each thread has its own pool
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let barrier_clone = barrier.clone();

        let thread1 = thread::spawn(move || {
            let _buf = get_buffer();
            barrier_clone.wait();

            // Hold buffer while other thread runs
            barrier_clone.wait();
        });

        let thread2 = thread::spawn(move || {
            barrier.wait();

            // Should get buffer from thread2's pool, not affected by thread1
            let buf = get_buffer();
            assert_eq!(buf.len(), DEFAULT_BUFFER_SIZE);

            barrier.wait();
        });

        thread1.join().unwrap();
        thread2.join().unwrap();
    }

    #[test]
    fn test_concurrent_threads() {
        // RED PHASE: Stress test with 10 threads
        let handles: Vec<_> = (0..10)
            .map(|i| {
                thread::spawn(move || {
                    // Each thread acquires and releases buffers
                    for _ in 0..100 {
                        let mut buf = get_buffer();

                        // Write some data
                        buf[0] = i as u8;

                        // Verify buffer size
                        assert_eq!(buf.len(), DEFAULT_BUFFER_SIZE);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_buffer_modification() {
        // RED PHASE: Test that buffers can be modified
        let pool = BufferPool::new(5, 128);

        let mut buf = pool.acquire();

        // Modify buffer
        buf[0] = 0xFF;
        buf[127] = 0xAA;

        assert_eq!(buf[0], 0xFF);
        assert_eq!(buf[127], 0xAA);

        drop(buf);

        // Acquire again - should be zeroed
        let buf2 = pool.acquire();
        assert_eq!(buf2[0], 0);
        assert_eq!(buf2[127], 0);
    }
}
