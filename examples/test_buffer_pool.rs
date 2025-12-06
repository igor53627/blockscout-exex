//! Standalone test for buffer_pool module
//! Run with: cargo run --example test_buffer_pool

use blockscout_exex::buffer_pool::{BufferPool, DEFAULT_BUFFER_SIZE, DEFAULT_POOL_SIZE, get_buffer};
use std::sync::Arc;
use std::thread;

fn main() {
    println!("Testing Buffer Pool Implementation");
    println!("==================================\n");

    test_buffer_acquire_return();
    test_buffer_guard_drop();
    test_pool_exhaustion();
    test_thread_local_isolation();
    test_concurrent_threads();
    test_buffer_modification();

    println!("\n✅ All tests passed!");
}

fn test_buffer_acquire_return() {
    println!("Test: buffer_acquire_return");
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
    println!("  ✓ Buffer acquire and return works correctly");
}

fn test_buffer_guard_drop() {
    println!("Test: buffer_guard_drop (RAII)");
    let pool = BufferPool::new(5, 512);

    let initial_available = pool.stats().available;
    assert_eq!(initial_available, 5);

    {
        let _guard = pool.acquire();
        assert_eq!(pool.stats().available, 4);
    } // Guard dropped

    // Buffer should be returned automatically
    assert_eq!(pool.stats().available, 5);
    println!("  ✓ RAII pattern works correctly");
}

fn test_pool_exhaustion() {
    println!("Test: pool_exhaustion");
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
    println!("  ✓ Pool exhaustion fallback works correctly");
}

fn test_thread_local_isolation() {
    println!("Test: thread_local_isolation");
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
    println!("  ✓ Thread-local isolation works correctly");
}

fn test_concurrent_threads() {
    println!("Test: concurrent_threads (10 threads)");
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
    println!("  ✓ Concurrent threads work correctly");
}

fn test_buffer_modification() {
    println!("Test: buffer_modification");
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
    println!("  ✓ Buffer modification and zeroing works correctly");
}
