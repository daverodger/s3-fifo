use lru::LruCache;
use std::num::NonZeroUsize;
use surrealkv::storage::cache::s3fifo::Cache;
use std::{fs};
use std::fs::File;
use std::io::{BufRead, BufReader, Result, Lines, Write};
use std::path::PathBuf;


fn main() -> Result<()> {
    match fs::read_dir("benches/data") {
        Err(e) => eprintln!("{:?}", e.kind()),
        Ok(paths) => {
            for path in paths {
                let path = path?;
                let unique_page_count: usize = {
                    match path.file_name().to_str().unwrap() {
                        "DS1.lis" => 10516352,
                        "OLTP.lis" => 18688, // reduced by factor of 10
                        "P1.lis" => 2311485,
                        "S1.lis" => 1309698,
                        "S3.lis" => 1689882,
                        _ => panic!("could not match filename to unique count")
                    }
                };
                let cache_diff = unique_page_count / 100;
                let mut cache_sizes = Vec::new();
                for i in 1..100 {
                    cache_sizes.push(cache_diff * i);
                }
                println!("Collecting Simulation Data for {:?}", path.file_name());
                let mut buffer = File::create(format!("benches/results/{:?}", path.file_name()))?;
                for size in cache_sizes {
                    buffer.write_all(calculate_hit_rate(path.path(), NonZeroUsize::new(size).unwrap())?.as_bytes())?;
                }
            }
        }
    }
    Ok(())
}

fn calculate_hit_rate(data: PathBuf, size: NonZeroUsize) -> Result<String> {
    let nums = read_lines(data)?.map(|line|
        line
            .unwrap()
            .split_ascii_whitespace()
            .take(2)
            .collect::<Vec<&str>>()
            .iter().map(|x| u64::from_str_radix(x, 10).unwrap())
            .collect::<Vec<u64>>()
    ).collect::<Vec<Vec<u64>>>();
    let mut s3fifo = Cache::new(size);
    let mut lru = LruCache::new(size);
    let mut request_count = 0;
    let mut hits_s3fifo: u64 = 0;
    let mut hits_lru: u64 = 0;
    for num in &nums {
        let new_val = num.get(0).expect("zero index guaranteed").to_owned();
        let seq = num.get(1).expect("one index guaranteed").to_owned();
        for x in 0..seq {
            request_count += 1;
            if let None = s3fifo.get(&(new_val + x)) {
                s3fifo.insert(new_val + x, new_val + x);
            } else {
                hits_s3fifo += 1;
            }
            if let None = lru.get(&(new_val + x)) {
                lru.put(new_val + x, new_val + x);
            } else {
                hits_lru += 1;
            }
        }
    }
    Ok(format!("{},{}\n",
               hits_s3fifo * 100 / request_count as u64,
               hits_lru * 100 / request_count as u64))
}

fn read_lines(filename: PathBuf) -> Result<Lines<BufReader<File>>> {
    let file = File::open(filename)?;
    Ok(BufReader::new(file).lines())
}