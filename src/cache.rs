use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use dashmap::DashMap;
use parking_lot::Mutex;
use serde_json;

/// Trait for cache entries that can estimate their memory footprint
pub trait CacheEntry: Send + Sync + Clone {
    fn size_bytes(&self) -> usize;
}

/// Memory-bounded LRU cache with DashMap for concurrent access
pub struct LruCache<K: Hash + Eq, V: CacheEntry> {
    /// Key -> (Value, LastAccess timestamp)
    data: DashMap<K, (V, Instant)>,
    /// Total bytes currently stored
    size_tracker: AtomicUsize,
    /// Maximum allowed bytes (default 1GB)
    max_size: usize,
    /// Serialize eviction operations
    eviction_lock: Mutex<()>,
}

impl<K: Hash + Eq, V: CacheEntry> LruCache<K, V> {
    /// Create a new LRU cache with the given maximum size in bytes
    pub fn new(max_size: usize) -> Self {
        Self {
            data: DashMap::new(),
            size_tracker: AtomicUsize::new(0),
            max_size,
            eviction_lock: Mutex::new(()),
        }
    }

    /// Get a value from the cache, updating its access time
    pub fn get(&self, key: &K) -> Option<V>
    where
        K: Clone,
    {
        self.data.get_mut(key).map(|mut entry| {
            // Update access time
            entry.1 = Instant::now();
            entry.0.clone()
        })
    }

    /// Insert a value into the cache
    pub fn insert(&self, key: K, value: V)
    where
        K: Clone,
    {
        let entry_size = value.size_bytes();
        let now = Instant::now();

        // Insert or update entry
        if let Some(mut old_entry) = self.data.get_mut(&key) {
            let old_size = old_entry.0.size_bytes();
            old_entry.0 = value;
            old_entry.1 = now;

            // Update size tracker
            if entry_size > old_size {
                self.size_tracker.fetch_add(entry_size - old_size, Ordering::Relaxed);
            } else {
                self.size_tracker.fetch_sub(old_size - entry_size, Ordering::Relaxed);
            }
        } else {
            self.data.insert(key, (value, now));
            self.size_tracker.fetch_add(entry_size, Ordering::Relaxed);
        }

        // Check if eviction needed
        if self.size_tracker.load(Ordering::Relaxed) > self.max_size {
            self.evict_to_threshold();
        }
    }

    /// Evict oldest entries until cache is at 90% of max_size
    fn evict_to_threshold(&self)
    where
        K: Clone,
    {
        // Use lock to serialize evictions
        let _guard = self.eviction_lock.lock();

        // Target size is 90% of max
        let target_size = (self.max_size * 9) / 10;

        // Keep evicting until we're under target
        while self.size_tracker.load(Ordering::Relaxed) > target_size {
            // Find the oldest entry
            let oldest = self.data.iter()
                .min_by_key(|entry| entry.value().1)
                .map(|entry| entry.key().clone());

            if let Some(key) = oldest {
                if let Some((_, (value, _))) = self.data.remove(&key) {
                    let size = value.size_bytes();
                    self.size_tracker.fetch_sub(size, Ordering::Relaxed);
                }
            } else {
                // No entries left to evict
                break;
            }
        }
    }

    /// Get current cache size in bytes
    pub fn size_bytes(&self) -> usize {
        self.size_tracker.load(Ordering::Relaxed)
    }

    /// Get number of entries in cache
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Clear all entries from the cache
    pub fn clear(&self) {
        self.data.clear();
        self.size_tracker.store(0, Ordering::Relaxed);
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            entries: self.len(),
            size_bytes: self.size_bytes(),
            max_size: self.max_size,
            utilization_percent: (self.size_bytes() as f64 / self.max_size as f64 * 100.0),
        }
    }
}

/// Cache statistics for monitoring
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: usize,
    pub size_bytes: usize,
    pub max_size: usize,
    pub utilization_percent: f64,
}

// ========== CacheEntry Implementations ==========

/// Generic implementation for Vec<u8>
impl CacheEntry for Vec<u8> {
    fn size_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.len()
    }
}

/// Generic implementation for String
impl CacheEntry for String {
    fn size_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.len()
    }
}

/// Generic implementation for serde_json::Value
impl CacheEntry for serde_json::Value {
    fn size_bytes(&self) -> usize {
        // Approximate size based on serialized representation
        match self {
            serde_json::Value::Null => std::mem::size_of::<Self>(),
            serde_json::Value::Bool(_) => std::mem::size_of::<Self>(),
            serde_json::Value::Number(_) => std::mem::size_of::<Self>() + 8,
            serde_json::Value::String(s) => std::mem::size_of::<Self>() + s.len(),
            serde_json::Value::Array(arr) => {
                std::mem::size_of::<Self>() + arr.iter().map(|v| v.size_bytes()).sum::<usize>()
            }
            serde_json::Value::Object(obj) => {
                std::mem::size_of::<Self>()
                    + obj.iter().map(|(k, v)| k.len() + v.size_bytes()).sum::<usize>()
            }
        }
    }
}

// ========== Typed Cache Constructors ==========

/// Default cache size: 1GB
pub const DEFAULT_MAX_SIZE: usize = 1024 * 1024 * 1024;

/// Create a cache for JSON values (blocks, transactions, receipts, etc.)
pub fn new_json_cache(max_size: usize) -> LruCache<String, serde_json::Value> {
    LruCache::new(max_size)
}

/// Create a cache for byte arrays
pub fn new_bytes_cache(max_size: usize) -> LruCache<String, Vec<u8>> {
    LruCache::new(max_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Simple test struct
    #[derive(Clone, Debug, PartialEq)]
    struct TestEntry {
        data: Vec<u8>,
    }

    impl CacheEntry for TestEntry {
        fn size_bytes(&self) -> usize {
            std::mem::size_of::<Self>() + self.data.len()
        }
    }

    // Test 1: Verify CacheEntry trait implementation
    #[test]
    fn test_cache_entry_size_estimation() {
        let entry = TestEntry {
            data: vec![0u8; 100],
        };

        // Should return size of struct + data length
        let size = entry.size_bytes();
        assert!(size >= 100, "Size should be at least 100 bytes for the vec data");
    }

    // Test 2: Verify different sizes
    #[test]
    fn test_cache_entry_varying_sizes() {
        let small = TestEntry { data: vec![0u8; 10] };
        let large = TestEntry { data: vec![0u8; 1000] };

        assert!(large.size_bytes() > small.size_bytes());
        assert_eq!(large.size_bytes() - small.size_bytes(), 990);
    }

    // Test 3: Empty cache entry
    #[test]
    fn test_cache_entry_empty() {
        let empty = TestEntry { data: vec![] };
        let size = empty.size_bytes();

        // Should still have struct overhead
        assert!(size > 0, "Empty entry should have struct overhead");
    }

    // Test 4: Clone preserves size
    #[test]
    fn test_cache_entry_clone_preserves_size() {
        let original = TestEntry { data: vec![42u8; 50] };
        let cloned = original.clone();

        assert_eq!(original.size_bytes(), cloned.size_bytes());
    }

    // Test 5: String-based entry
    #[derive(Clone)]
    struct StringEntry {
        text: String,
    }

    impl CacheEntry for StringEntry {
        fn size_bytes(&self) -> usize {
            std::mem::size_of::<Self>() + self.text.len()
        }
    }

    #[test]
    fn test_string_cache_entry() {
        let entry = StringEntry {
            text: "Hello, World!".to_string(),
        };

        let size = entry.size_bytes();
        assert!(size >= 13, "Size should include string length");
    }

    // ========== LruCache Tests ==========

    // Test 1: Basic insert and get
    #[test]
    fn test_cache_insert_get() {
        let cache = LruCache::new(1024);
        let entry = TestEntry { data: vec![1, 2, 3] };

        cache.insert(1, entry.clone());

        let retrieved = cache.get(&1);
        assert_eq!(retrieved, Some(entry));
        assert_eq!(cache.len(), 1);
    }

    // Test 2: Size tracking accuracy
    #[test]
    fn test_size_tracking() {
        let cache = LruCache::new(10000);

        let entry1 = TestEntry { data: vec![0u8; 100] };
        let entry2 = TestEntry { data: vec![0u8; 200] };

        cache.insert("key1", entry1.clone());
        let size_after_1 = cache.size_bytes();
        assert!(size_after_1 >= 100, "Size should include first entry");

        cache.insert("key2", entry2.clone());
        let size_after_2 = cache.size_bytes();
        assert!(size_after_2 > size_after_1, "Size should increase with second entry");

        // Size should be approximately sum of both entries
        let expected_min = entry1.size_bytes() + entry2.size_bytes();
        assert!(cache.size_bytes() >= expected_min, "Total size should be at least sum of entries");
    }

    // Test 3: Empty cache
    #[test]
    fn test_empty_cache() {
        let cache: LruCache<i32, TestEntry> = LruCache::new(1024);

        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.size_bytes(), 0);
        assert_eq!(cache.get(&1), None);
    }

    // Test 4: Update existing key
    #[test]
    fn test_update_existing_key() {
        let cache = LruCache::new(10000);

        let entry1 = TestEntry { data: vec![0u8; 100] };
        let entry2 = TestEntry { data: vec![0u8; 200] };

        cache.insert("key", entry1.clone());
        let size_1 = cache.size_bytes();

        cache.insert("key", entry2.clone());
        let size_2 = cache.size_bytes();

        // Should have only one entry
        assert_eq!(cache.len(), 1);

        // Size should reflect the new larger entry
        assert!(size_2 > size_1);

        let retrieved = cache.get(&"key");
        assert_eq!(retrieved, Some(entry2));
    }

    // Test 5: Concurrent access with multiple threads
    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(LruCache::new(100000));
        let mut handles = vec![];

        for thread_id in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                for i in 0..10 {
                    let key = thread_id * 100 + i;
                    let entry = TestEntry {
                        data: vec![thread_id as u8; 10],
                    };
                    cache_clone.insert(key, entry.clone());

                    // Verify we can read back
                    let retrieved = cache_clone.get(&key);
                    assert_eq!(retrieved, Some(entry));
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should have 100 entries (10 threads * 10 inserts each)
        assert_eq!(cache.len(), 100);
    }

    // ========== Eviction Tests ==========

    // Test 1: LRU eviction when cache exceeds limit
    #[test]
    fn test_lru_eviction() {
        use std::thread;
        use std::time::Duration;

        // Small cache: 500 bytes max
        let cache = LruCache::new(500);

        // Insert entries with delays to establish clear LRU order
        let entry1 = TestEntry { data: vec![0u8; 100] };
        cache.insert("old1", entry1.clone());
        thread::sleep(Duration::from_millis(10));

        let entry2 = TestEntry { data: vec![0u8; 100] };
        cache.insert("old2", entry2.clone());
        thread::sleep(Duration::from_millis(10));

        let entry3 = TestEntry { data: vec![0u8; 100] };
        cache.insert("recent1", entry3.clone());
        thread::sleep(Duration::from_millis(10));

        // Access "old2" to make it more recent
        cache.get(&"old2");
        thread::sleep(Duration::from_millis(10));

        // Now insert large entry that triggers eviction
        let large_entry = TestEntry { data: vec![0u8; 250] };
        cache.insert("trigger", large_entry.clone());

        // "old1" should be evicted (oldest), but others should remain
        assert_eq!(cache.get(&"old1"), None, "Oldest entry should be evicted");

        // These should still exist
        assert!(cache.get(&"old2").is_some(), "Recently accessed should remain");
        assert!(cache.get(&"recent1").is_some(), "Recent entry should remain");
        assert!(cache.get(&"trigger").is_some(), "New entry should exist");
    }

    // Test 2: Eviction threshold (90%)
    #[test]
    fn test_eviction_threshold() {
        let cache = LruCache::new(1000);

        // Fill cache to just over capacity
        for i in 0..20 {
            let entry = TestEntry { data: vec![0u8; 100] };
            cache.insert(i, entry);
        }

        // Cache should evict down to 90% (900 bytes)
        let final_size = cache.size_bytes();
        assert!(final_size <= 900, "Cache should evict to 90% threshold, got {}", final_size);
    }

    // Test 3: No eviction when under limit
    #[test]
    fn test_no_eviction_under_limit() {
        let cache = LruCache::new(10000);

        // Insert well under capacity
        for i in 0..5 {
            let entry = TestEntry { data: vec![0u8; 100] };
            cache.insert(i, entry);
        }

        // All entries should remain
        assert_eq!(cache.len(), 5);
        for i in 0..5 {
            assert!(cache.get(&i).is_some(), "Entry {} should not be evicted", i);
        }
    }

    // Test 4: Eviction preserves most recent entries
    #[test]
    fn test_eviction_preserves_recent() {
        use std::thread;
        use std::time::Duration;

        let cache = LruCache::new(500);

        // Insert old entries
        for i in 0..3 {
            let entry = TestEntry { data: vec![0u8; 100] };
            cache.insert(i, entry);
            thread::sleep(Duration::from_millis(5));
        }

        // Insert new entries with larger size to trigger eviction
        for i in 100..103 {
            let entry = TestEntry { data: vec![0u8; 100] };
            cache.insert(i, entry);
            thread::sleep(Duration::from_millis(5));
        }

        // Old entries should be evicted
        assert!(cache.get(&0).is_none() || cache.get(&1).is_none() || cache.get(&2).is_none(),
                "Some old entries should be evicted");

        // New entries should remain
        for i in 100..103 {
            assert!(cache.get(&i).is_some(), "Recent entry {} should remain", i);
        }
    }

    // Test 5: Concurrent eviction safety
    #[test]
    fn test_concurrent_eviction() {
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(LruCache::new(2000));
        let mut handles = vec![];

        // Multiple threads inserting concurrently to trigger evictions
        for thread_id in 0..5 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                for i in 0..20 {
                    let key = thread_id * 1000 + i;
                    let entry = TestEntry { data: vec![0u8; 100] };
                    cache_clone.insert(key, entry);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Cache should respect size limit
        assert!(cache.size_bytes() <= 2000, "Cache size {} exceeds limit", cache.size_bytes());

        // Cache should still function correctly
        let test_entry = TestEntry { data: vec![42u8; 50] };
        cache.insert(9999, test_entry.clone());
        assert_eq!(cache.get(&9999), Some(test_entry));
    }

    // ========== Typed Cache Tests ==========

    // Test 1: JSON cache with real data
    #[test]
    fn test_json_cache() {
        use serde_json::json;

        let cache = new_json_cache(10000);

        let block_data = json!({
            "number": "0x1234",
            "hash": "0xabcd...",
            "transactions": ["0xtx1", "0xtx2"]
        });

        cache.insert("block_1234".to_string(), block_data.clone());

        let retrieved = cache.get(&"block_1234".to_string());
        assert_eq!(retrieved, Some(block_data));
    }

    // Test 2: String cache
    #[test]
    fn test_string_cache() {
        let cache: LruCache<String, String> = LruCache::new(1000);

        cache.insert("key1".to_string(), "value1".to_string());
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
    }

    // Test 3: Bytes cache
    #[test]
    fn test_bytes_cache() {
        let cache = new_bytes_cache(1000);

        let data = vec![1, 2, 3, 4, 5];
        cache.insert("data1".to_string(), data.clone());

        assert_eq!(cache.get(&"data1".to_string()), Some(data));
    }

    // Test 4: Cache stats
    #[test]
    fn test_cache_stats() {
        let cache = LruCache::new(1000);

        let entry = TestEntry { data: vec![0u8; 100] };
        cache.insert("key", entry);

        let stats = cache.stats();
        assert_eq!(stats.entries, 1);
        assert!(stats.size_bytes > 0);
        assert_eq!(stats.max_size, 1000);
        assert!(stats.utilization_percent > 0.0);
        assert!(stats.utilization_percent <= 100.0);
    }

    // Test 5: Clear cache
    #[test]
    fn test_clear_cache() {
        let cache = LruCache::new(1000);

        for i in 0..10 {
            let entry = TestEntry { data: vec![0u8; 50] };
            cache.insert(i, entry);
        }

        assert_eq!(cache.len(), 10);
        assert!(cache.size_bytes() > 0);

        cache.clear();

        assert_eq!(cache.len(), 0);
        assert_eq!(cache.size_bytes(), 0);
        assert!(cache.is_empty());
    }
}
