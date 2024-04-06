/// This is an experimental implementation of a cache that uses the S3-FIFO algorithm. This is not yet
/// used in the main codebase. But the implementation is kept here for future reference for replacing it
/// with the current LRU cache for caching recently accessed values.
use hashbrown::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::cmp::max;
use ringbuf::{HeapRb, Rb};
use indexmap::IndexSet;

/// Maximum frequency limit for an entry in the cache.
const MAX_FREQUENCY_LIMIT: u8 = 3;

/// Represents an entry in the cache.
#[derive(Debug)]
struct Entry<K, V> {
    key: K,
    value: V,
    /// Frequency of access of this entry.
    freq: AtomicU8,
}

impl<K, V> Entry<K, V> {
    /// Creates a new entry with the given key and value.
    pub fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            freq: AtomicU8::new(0),
        }
    }
}

impl<K, V> Clone for Entry<K, V>
    where
        K: Clone,
        V: Clone,
{
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            value: self.value.clone(),
            freq: AtomicU8::new(self.freq.load(Relaxed)),
        }
    }
}

struct GhostQueue<K> {
    queue: IndexSet<K>,
    capacity: usize,
}

impl<K: Hash + Eq + PartialEq + Clone> GhostQueue<K> {
    fn new(size: usize) -> Self {
        Self {
            queue: IndexSet::with_capacity(size),
            capacity: size,
        }
    }

    fn push(&mut self, key: K) {
        if self.queue.len() == self.capacity {
            self.evict()
        }
        self.queue.insert(key);
    }

    fn evict(&mut self) {
        self.queue.pop();
    }

    fn contains(&self, key: &K) -> bool {
        self.queue.contains(key)
    }
}

/// Cache is an implementation of "S3-FIFO" from "FIFO Queues are ALL You Need for Cache Eviction" by
/// Juncheng Yang, et al. <https://jasony.me/publication/sosp23-s3fifo.pdf>
pub struct Cache<K, V>
    where
        K: PartialEq + Eq + Hash + Clone + Debug,
        V: Clone + Debug,
{
    /// Small queue for entries with low frequency.
    small: HeapRb<K>,
    /// Main queue for entries with high frequency.
    main: HeapRb<K>,
    /// Ghost queue for evicted entry keys.
    ghost: GhostQueue<K>,
    /// Map of all entries for quick access.
    entries: HashMap<K, Entry<K, V>>,
}

impl<K, V> Cache<K, V>
    where
        K: PartialEq + Eq + Hash + Clone + Debug,
        V: Clone + Debug,
{
    /// Creates a new cache with the given maximum size.
    pub fn new(max_cache_size: usize) -> Self {
        let max_small_size = max(max_cache_size / 10, 1);
        let max_main_size = max(max_cache_size - max_small_size, 1);

        Self {
            small: HeapRb::new(max_small_size),
            main: HeapRb::new(max_main_size),
            ghost: GhostQueue::new(max_main_size),
            entries: HashMap::new(),
        }
    }

    /// Returns a reference to the value of the given key if it exists in the cache.
    pub fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(entry) = self.entries.get(key) {
            let freq = entry.freq.load(Acquire);
            if freq < MAX_FREQUENCY_LIMIT {
                entry.freq.store(freq + 1, Release);
            }
            Some(&entry.value)
        } else {
            None
        }
    }

    /// Inserts a new entry with the given key and value into the cache.
    pub fn insert(&mut self, key: K, value: V) -> bool {
        if self.entries.contains_key(&key) {
            return false;
        }
        if self.ghost.contains(&key) {
            self.insert_m(key.clone());
        } else {
            self.insert_s(key.clone());
        }
        let entry = Entry::new(key.clone(), value);
        self.entries.insert(key, entry);
        true
    }

    fn insert_s(&mut self, key: K) {
        if let Some(victim) = self.small.push_overwrite(key.clone()) {
            match self.entries.get(&victim).unwrap().freq.load(Relaxed) {
                0 => {
                    self.entries.remove(&victim);
                    self.insert_g(victim);
                }
                _ => {
                    let entry = self.entries.get(&victim).unwrap();
                    entry.freq.store(0, Relaxed);
                    self.insert_m(victim);
                }
            }
        }
    }
    fn insert_m(&mut self, key: K) {
        if let Some(victim) = self.main.push_overwrite(key) {
            match self.entries.get(&victim).unwrap().freq.load(Relaxed) {
                0 => {
                    self.entries.remove(&victim);
                }
                _ => {
                    self.insert_m({
                        self.entries.get(&victim).unwrap().freq.fetch_sub(1, Relaxed);
                        victim
                    });
                    self.evict_m();
                }
            }
        }
    }

    fn insert_g(&mut self, key: K) {
        self.ghost.push(key);
    }

    fn evict_m(&mut self) {
        let mut evicted = false;
        while !evicted && self.main.len() > 0 {
            let victim = self.main.pop().unwrap();
            match self.entries.get(&victim).unwrap().freq.load(Relaxed) {
                0 => {
                    self.entries.remove(&victim);
                    evicted = true;
                }
                _ => {
                    self.insert_m({
                        self.entries.get(&victim).unwrap().freq.fetch_sub(1, Relaxed);
                        victim
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::thread;
    use rand::{Rng, thread_rng};

    use super::*;

    fn assert_opt_eq<V: PartialEq + Debug>(opt: Option<&V>, v: V) {
        assert!(opt.is_some());
        assert_eq!(opt.unwrap(), &v);
    }

    #[test]
    fn test_push_and_read() {
        let mut cache = Cache::new(2);

        cache.insert("apple", "red");
        assert_opt_eq(cache.get(&"apple"), "red");
        cache.insert("banana", "yellow");
        assert_opt_eq(cache.get(&"apple"), "red");
        assert_opt_eq(cache.get(&"banana"), "yellow");
    }

    #[test]
    fn test_push_removes_oldest() {
        let mut cache = Cache::new(2);

        let fruits = vec![
            ("apple", "red"),
            ("banana", "yellow"),
            ("orange", "orange"),
            ("pear", "green"),
            ("peach", "pink"),
        ];

        for (fruit, color) in fruits {
            cache.insert(fruit, color);
        }

        assert!(cache.get(&"apple").is_none());
        assert_opt_eq(cache.get(&"peach"), "pink");

        // "apple" should have been removed from the cache.
        cache.insert("apple", "red");
        cache.get(&"apple");
        cache.insert("banana", "yellow");

        // assert!(cache.get(&"pear").is_none());
        assert_opt_eq(cache.get(&"apple"), "red");
        assert_opt_eq(cache.get(&"banana"), "yellow");
    }

    #[test]
    fn test_concurrent() {
        let cache = Arc::new(Mutex::new(Cache::new(2)));
        for i in 0..1000 {
            let i_cache = Arc::clone(&cache);
            thread::spawn(move || i_cache.lock().unwrap().insert(i, i));
            let g_cache = Arc::clone(&cache);
            thread::spawn(move || { g_cache.lock().unwrap().get(&i); });
        };
    }

    #[test]
    fn test_rng_criterion() {
        let mut rng = thread_rng();
        let nums: Vec<u64> =
            (0..(100000 * 2))
                .map(|i| {
                    if i % 2 == 0 {
                        rng.gen::<u64>() % 16384
                    } else {
                        rng.gen::<u64>() % 32768
                    }
                })
                .collect()
            ;
        let mut l = Cache::new(8192);
        (0..99999).for_each(|v| {
            let k = nums[v];
            l.insert(k, k);
        });
    }

    #[test]
    fn test_simple_dupe() {
        let mut cache = Cache::new(20);
        cache.insert("test", "first");
        cache.insert("test", "second");
        cache.insert("bug", "third");
        assert!(cache.get(&"test").is_some());
    }

    #[test]
    fn test_no_memory_leaks() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        #[derive(Debug, Clone)]
        struct DropCounter;

        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Relaxed);
            }
        }

        let n = 100;
        for _ in 0..n {
            let mut cache = Cache::new(20);
            for i in 0..n {
                cache.insert(i, DropCounter {});
            }
        }
        assert_eq!(DROP_COUNT.load(Relaxed), n * n);
    }
}
