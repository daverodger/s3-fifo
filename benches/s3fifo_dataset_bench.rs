use lru::LruCache;
use std::num::NonZeroUsize;
use surrealkv::storage::cache::s3fifo::Cache;
use std::{fs, io};
use std::fs::File;
use std::io::{BufRead, BufReader, Result, Lines};
use std::path::PathBuf;


fn main() -> io::Result<()> {
    let cache_sizes = [125_000, 12_500, 1250, 125];
    match fs::read_dir("benches/data") {
        Err(e) => eprintln!("{:?}", e.kind()),
        Ok(paths) => {
            for path in paths {
                let path = path?;
                println!("Running Simulation: {}", path.path().to_str().unwrap());
                for size in cache_sizes {
                    println!("{}kB Cache Size", size);
                    let _ = calculate_hit_rate(path.path(), NonZeroUsize::new(size).unwrap())?;
                }
            }
        }
    }
    println!();
    Ok(())
}

fn calculate_hit_rate(data: PathBuf, size: NonZeroUsize) -> Result<()> {
    let nums = read_lines(data)?.map(|line|
        u64::from_str_radix(
            line
                .unwrap()
                .split_ascii_whitespace()
                .take(1)
                .next()
                .unwrap()
            , 10,
        ).unwrap()
    ).collect::<Vec<u64>>();
    let mut s3fifo = Cache::new(size);
    let mut lru = LruCache::new(size);
    let mut hits_s3fifo: u64 = 0;
    let mut hits_lru: u64 = 0;
    for num in &nums {
        if let None = s3fifo.get(&num) {
            s3fifo.insert(num, num);
        } else {
            hits_s3fifo += 1;
        }
        if let None = lru.get(&num) {
            lru.put(num, num);
        } else {
            hits_lru += 1;
        }
    }
    println!("S3-FIFO hit rate: {}%", hits_s3fifo * 100 / nums.len() as u64);
    println!("LRU hit rate: {}%", hits_lru * 100 / nums.len() as u64);
    Ok(())
}

fn read_lines(filename: PathBuf) -> Result<Lines<BufReader<File>>> {
    let file = File::open(filename)?;
    Ok(BufReader::new(file).lines())
}