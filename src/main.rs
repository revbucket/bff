use ahash::RandomState;
use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, NativeEndian, ReadBytesExt, WriteBytesExt};
use clap::{Parser, Subcommand};
use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use rand::{Rng, thread_rng};
use rand::seq::SliceRandom;
use serde_json::Value;
use std::clone::Clone;
use std::collections::VecDeque;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Write, Cursor};
use std::mem::size_of;
use std::path::{PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread::available_parallelism;
use threadpool::ThreadPool;
use unicode_segmentation::UnicodeSegmentation;
use rayon::prelude::*;
use sysinfo::{
    System,
};
use glob::glob;
use human_bytes::human_bytes;
use std::fs::{OpenOptions, create_dir_all};
use std::sync::{Arc, Mutex};
use indicatif::{ProgressBar,ProgressStyle};
use std::time::{Instant};

use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{Client};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader as tBufReader;
use tokio::time::{Duration, sleep};
use async_compression::tokio::bufread::GzipDecoder as asyncGZ;
use async_compression::tokio::bufread::ZstdDecoder as asyncZstd;


#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct ArgParser {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[clap(arg_required_else_help = true)]
    Bff {
        /// (List of) directories or files that are jsonl.gz files
        #[arg(required=true, long)]
        inputs: Vec<PathBuf>,

        /// Output directory where the deduplicated files will end up. 
        /// These will have the same basename as the inputs, so it is up to you to ensure no collisions here!
        #[arg(required=true, long)]
        output_directory: PathBuf,    

        /// If specified, tries to load the bloom filter from this file, and will save once complete.
        /// If unspecified, will not save the bloom filter at the end    
        #[arg(long)]
        bloom_filter_file: Option<PathBuf>,

        /// The number of expected ngrams. This is used to calculate the optimal number of hashers.
        /// If the filter already exists, this parameter is ignored.
        #[arg(required=true, long)]
        expected_ngram_count: usize,

        /// The desired false positive rate
        /// Note that this is a per-ngram FP rate, and not a per-paragraph rate
        #[arg(required=true, long)]
        fp_rate: f64,    

        /// The smallest ngram size to consider. Paragraphs that have fewer than this number of tokens
        /// are not deduplicated and always kept. These ngrams are never added to the bloom filter.
        /// Note that this value only matters if the paragraph has fewer tokens than the max ngram size.
        #[arg(long, default_value_t = 20)]
        min_ngram_size: usize,

        /// The largest ngram size to consider. Paragraphs are deduplicated based on the number of
        /// ngrams of this size that are already present in the bloom filter.
        #[arg(long, default_value_t = 20)]
        max_ngram_size: usize,

        /// If this fraction of ngrams of the max ngram size are already present in the bloom filter,
        /// the paragraph is considered a duplicate and is discarded.
        /// Set this to 0 to never produce any output. This is useful when you want to prime the filter
        /// with some content that should be considered duplicates, without deduplicating that content
        /// itself.
        #[arg(long, default_value_t = 0.80)]
        filtering_threshold: f64,

        /// How many tokens to count as a duplicate in substring mode
        #[arg(long, default_value_t = 50)]
        substr_seqlen: usize,

        /// Which "BFF mode" we're in. We have options of 'paragraph', 'document', 'both'
        /// indicating we remove individual paragraphs/documents if duplicated
        /// The logic for "both" mode is a bit subtle. See comments below
        #[arg(long, default_value_t = RemoveType::Paragraph, value_enum)]
        remove_type: RemoveType,        


        /// Number of hashers to use in bloom filter 
        /// 0 is the default in which case we use the optimal number 
        #[arg(long, default_value_t = 0)]
        num_hashers: usize,

        /// Whether or not to update the bloom filter. If this is true, the filter is not updated, but
        /// the input is still deduplicated based on the filter. Default is false.
        #[arg(long, default_value_t = false)]
        no_update_bloom_filter: bool,

        /// If this is true, we keep the input intact, but we add an annotation to each document that
        /// explains which spans from the text would have been deleted.
        #[arg(long, default_value_t = false)]
        annotate: bool,

        /// The number of threads to use for processing.
        /// If this is 0, the number of threads is automatically determined.
        #[arg(long, short = 't', default_value_t = 0)]
        threads: usize,

        /// If this flag is present, we will never save a bloom filter to disk
        #[arg(long, default_value_t = false)]
        no_save_bloom_filter: bool,


        /// Turn this flag on if we don't want to use a progress bar 
        /// Helpful when running through ssh and wanting to check progress via logs and not a terminal
        #[arg(long, default_value_t = false)]
        no_progress_bar: bool,

        /// For virtual "sharding", this param and the next one subselect files to deduplicate together
        /// Defaults to no virtual sharding
         #[arg(long, default_value_t=0)]
        shard_num: usize,

        #[arg(long, default_value_t=1)]
        total_shards: usize,   


    },

    Sysreq {
        /// Handy tool to help guess RAM requirements for a given pool of data
        #[arg(required=true, long)]
        expected_ngram_count: usize,
        #[arg(required=true, long)]
        fp_rate: f64,
        #[arg(long, default_value_t=0)]
        num_hashers: usize,
    },
        
}

#[derive(Debug, Clone, Eq, PartialEq, clap::ValueEnum)]
enum RemoveType {
    /// Types for what we check to see if is a duplicate

    ///Checks each paragraph of size >=min_ngram_size if it is duplicated. If so, it gets removed
    Paragraph,

    /// Checks if enough of the ngrams of size ==max_ngram_size (or just one ngram if tokens in range [min_ngram_size, max_ngram_size])
    /// and if enough are present in filter, the whole document gets removed 
    Document,  

    /// Does paragraph removal, BUT if enough of the paragraph ngram checks are contained, removes the whole document
    Both,     

    /// Does substring style removal
    Substring,

    /// Does exact removal (it's not _really_ exact, shhh....)
    Exact,

}


fn not_whitespace(w: &str) -> bool {
    for c in w.chars() {
        if !c.is_whitespace() {
            return true;
        }
    }
    false
}


fn tokenize(s: &str) -> impl Iterator<Item = &str> {
    s.split_word_bounds().filter(|w| {not_whitespace(w)})
}

fn tokenize_indices(s: &str) -> impl Iterator<Item = (usize, &str)> {
    s.split_word_bound_indices().filter(|(_, w)| {not_whitespace(w)})
}



/*=============================================================
=                     Bloom Filter stuff                      =
==============================================================*/

struct BloomFilter {
    bits: Vec<AtomicU32>,
    hash_builder_seeds: Vec<[u64; 4]>, // RandomState does not store its seeds, so we have to store them ourselves.
    hash_builders: Vec<RandomState>,
}

impl BloomFilter {
    const MAGIC: u32 = 0x81F0_F117;
    const VERSION: u32 = 1;

    fn optimal_number_of_hashers(size_in_bytes: usize, expected_elements: usize) -> usize {
        let expected_elements = expected_elements as f64;
        let size_in_bits = (size_in_bytes * 8) as f64;
        let k = (size_in_bits / expected_elements) * (2.0f64.ln());
        k.ceil() as usize
    }

    fn prob_of_false_positive(
        size_in_bytes: usize,
        expected_elements: usize,
        num_hashers: usize,
    ) -> f64 {
        let k = num_hashers as f64;
        let m = (size_in_bytes * 8) as f64;
        let n = expected_elements as f64;
        (1.0 - (1.0 - (1.0 / m)).powf(k * n)).powf(k)
    }


    fn my_prob_of_false_positive(&self, expected_elements: usize) -> f64 {
        Self::prob_of_false_positive(
            self.size_in_bytes(),
            expected_elements,
            self.hash_builders.len(),
        )
    }

    fn size_in_bytes(&self) -> usize {
        self.bits.len() * size_of::<AtomicU32>()
    }

    fn calculate_sparsity(&self) -> f64 {
        let set_bits:usize = self.bits.par_iter()
            .map(|atomic| {
                let value = atomic.load(std::sync::atomic::Ordering::Relaxed);
                value.count_ones() as usize
            })
            .sum();
        let total_bits = self.size_in_bytes() * 8;
        return (set_bits as f64) / (total_bits as f64);
    }

    fn new(size_in_bytes: usize, num_hashers: usize) -> Self {
        let mut rng = rand::thread_rng();
        let mut hash_builder_seeds = Vec::with_capacity(num_hashers);
        let mut hash_builders = Vec::with_capacity(num_hashers);
        for _ in 0..num_hashers {
            let seeds = rng.gen::<[u64; 4]>();
            hash_builders.push(RandomState::with_seeds(
                seeds[0], seeds[1], seeds[2], seeds[3],
            ));
            hash_builder_seeds.push(seeds);
        }

        let number_of_u32 = size_in_bytes / size_of::<AtomicU32>();
        let bits = {
            (0..number_of_u32).into_par_iter().map(|_| AtomicU32::default()).collect()
        };


        Self {
            bits,
            hash_builder_seeds,
            hash_builders,
        }
    }



    fn from_file(path: &PathBuf) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(path)?;
        let mut stream = BufReader::new(&mut file);

        let magic: u32 = stream.read_u32::<LittleEndian>()?;
        if magic != Self::MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid magic"));
        }

        let version: u32 = stream.read_u32::<LittleEndian>()?;
        if version != Self::VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid version",
            ));
        }

        let num_hashers: u32 = stream.read_u32::<LittleEndian>()?;
        let mut hash_builder_seeds = Vec::with_capacity(num_hashers as usize);
        let mut hash_builders = Vec::with_capacity(num_hashers as usize);
        for _ in 0..num_hashers {
            let seeds = [
                stream.read_u64::<LittleEndian>()?,
                stream.read_u64::<LittleEndian>()?,
                stream.read_u64::<LittleEndian>()?,
                stream.read_u64::<LittleEndian>()?,
            ];
            hash_builders.push(RandomState::with_seeds(
                seeds[0], seeds[1], seeds[2], seeds[3],
            ));
            hash_builder_seeds.push(seeds);
        }

        let number_of_elements = stream.read_u64::<LittleEndian>()?;
        let mut bits = Vec::new();
        bits.reserve_exact(number_of_elements as usize);
        for _ in 0..number_of_elements {
            bits.push(AtomicU32::new(stream.read_u32::<NativeEndian>()?));
        }

        Ok(Self {
            bits,
            hash_builder_seeds,
            hash_builders,
        })
    }

    fn write_to_file(&self, path: &PathBuf) -> io::Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let mut stream = BufWriter::new(&file);

        stream.write_u32::<LittleEndian>(Self::MAGIC)?;
        stream.write_u32::<LittleEndian>(Self::VERSION)?;
        stream.write_u32::<LittleEndian>(self.hash_builder_seeds.len() as u32)?;
        for hash_builder_seed in &self.hash_builder_seeds {
            for seed in hash_builder_seed {
                stream.write_u64::<LittleEndian>(*seed)?;
            }
        }

        stream.write_u64::<LittleEndian>(self.bits.len() as u64)?;
        unsafe {
            let bytes: &[u8] = std::slice::from_raw_parts(
                self.bits.as_ptr().cast::<u8>(),
                self.bits.len() * size_of::<AtomicU32>(),
            );
            stream.write_all(bytes)?;
        };

        Ok(())
    }

    fn hashes(&self, s: &VecDeque<&str>) -> Vec<u64> {
        self.hash_builders
            .iter()
            .map(|hash_builder| {
                let mut hasher = hash_builder.build_hasher();
                s.hash(&mut hasher);
                hasher.finish()
            })
            .collect()
    }

    fn insert_hashes(&self, hashes: &Vec<u64>) {
        for hash in hashes {
            let hash = *hash as usize;
            let index = hash / 32 % self.bits.len();
            let bit = hash % 32;
            self.bits[index].fetch_or(1 << bit, Ordering::Relaxed);
        }
    }

    #[allow(dead_code)] // use in unit test
    fn insert(&self, s: &VecDeque<&str>) {
        let hashes = self.hashes(s);
        self.insert_hashes(&hashes);
    }

    fn contains_hashes(&self, hashes: &Vec<u64>) -> bool {
        for hash in hashes {
            let hash = *hash as usize;
            let index = hash / 32 % self.bits.len();
            let bit = hash % 32;
            if self.bits[index].load(Ordering::Relaxed) & (1 << bit) == 0 {
                return false;
            }
        }
        true
    }

    #[allow(dead_code)] // use in unit test
    fn contains(&self, s: &VecDeque<&str>) -> bool {
        let hashes = self.hashes(s);
        self.contains_hashes(&hashes)
    }


    fn from_args(bloom_filter_file: Option<PathBuf>, expected_ngram_count: usize, fp_rate: f64, num_hashers: usize) -> Self {
        /* Uses the CLI args to build a bloom filter 
        Logic:
            - Check if file exists, if so, just load it and return 
            - Get size:
                + if size is explicitly speciifed, use this
                + otherwise, compute based on ngrams + fp rate 
            - Return 
        */        

        let bloom_filter = match &bloom_filter_file {
            Some(path) if path.exists() => {
                println!("Loading bloom filter from {:?}...", path);
                BloomFilter::from_file(&path).unwrap()
            }
            _ => {
            println!("Creating new bloom filter...");
            let bloom_filter_size = compute_bloom_size(fp_rate, expected_ngram_count, true, num_hashers);
            let num_hashers = BloomFilter::optimal_number_of_hashers(
                bloom_filter_size,
                expected_ngram_count,
            );
            BloomFilter::new(bloom_filter_size, num_hashers)
            }
        };

        println!("Bloom filter has size {} | FP Rate {:?}",
                 human_bytes(bloom_filter.size_in_bytes() as f64), 
                 bloom_filter.my_prob_of_false_positive(expected_ngram_count));
        bloom_filter
    }

}



fn compute_bloom_size(fp_rate: f64, expected_ngram_count: usize, limit_to_sys: bool, num_hashers: usize) -> usize {
    /* Uses binary search to find optimal size of bloom filter using optimal number of hashers
       and provided ngram counts
    */
    // compute 90% of system ram 
    let mut sys = System::new_all();
    sys.refresh_all();


    let mut lo = 1 as usize;

    let mut hi = if limit_to_sys {
                    ((sys.total_memory() as f64) * 0.9) as usize
                 } else {
                    420_744_073_709_551_615 as usize
                 };

    let compute_hashers = num_hashers == 0;

    // Save some time by checking endpoint first
    let num_hashers = if compute_hashers {
         BloomFilter::optimal_number_of_hashers(hi, expected_ngram_count)
    } else {
        num_hashers
    };

    if limit_to_sys && BloomFilter::prob_of_false_positive(hi, expected_ngram_count, num_hashers) > fp_rate {
        println!(
            "WARNING: To achieve desired false-positive rate, you'd need >90% of system RAM. Defaulting to 90% \
            system RAM.");
        return hi;
    }

    // Then do binary search to find optimal size
    while lo < hi-1 {
        let mid = lo + (hi - lo) / 2;
        let num_hashers = if compute_hashers {
            BloomFilter::optimal_number_of_hashers(mid, expected_ngram_count)
        } else {num_hashers};
        let computed_fp = BloomFilter::prob_of_false_positive(mid, expected_ngram_count, num_hashers) ;
        if computed_fp > fp_rate {
            // FP rate too high, need to go bigger
            lo =  mid + 1;
        } else {
            // FP rate too low, can make bloom filter smaller
            hi = mid -1;
        }
    }
    hi
}





#[allow(clippy::too_many_arguments)] // TODO : abstract parameters into a struct
async fn process_file(
    input_file: &PathBuf,
    output_file: &PathBuf,
    bloom_filter: &Arc<BloomFilter>,
    max_ngram_size: usize,
    min_ngram_size: usize,
    substr_seqlen: usize,
    remove_type: &RemoveType,
    filtering_threshold: f64,
    no_update_bloom_filter: bool,    
    annotate: bool,
    pbar_option: &Option<Arc<Mutex<ProgressBar>>>,    
) -> Result<(usize, usize), io::Error> {

    // Setup input/output writers
    // If input file is local: can stream pretty easily/robustly
    // If input file is s3: load entire file and split it into thing that implements .lines() iterator
    let lines : Box<dyn Iterator<Item = Result<String, std::io::Error>>> = if is_s3(input_file) {
        /*
        let input_file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(input_file)?;
        BufReader::with_capacity(1024 * 1024, MultiGzDecoder::new(input_file)).lines()        
        */
        Box::new(get_reader_from_s3(input_file, None).await.unwrap().lines())
    } else {

        let input_file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(input_file)?;
        Box::new(BufReader::with_capacity(1024 * 1024, MultiGzDecoder::new(input_file)).lines())
    };

    // If output file is local, write directly to file
    let mut output_data: Vec<u8> = Vec::new();
    let mut writer = BufWriter::with_capacity(1024 * 1024, GzEncoder::new(Cursor::new(&mut output_data), Compression::default()));


    // Loop over lines and do bff stuff
    let mut count = 0;
    let mut fully_skipped = 0;
    let mut removed_text_bytes = 0;
    let mut total_text_bytes = 0;
    for line in lines {
        count += 1;
        let (dedup_data, removed_line_bytes, total_line_bytes) = 
        if *remove_type == RemoveType::Exact {
            process_line_exact(&line.unwrap(), &bloom_filter, no_update_bloom_filter, annotate)
        } else if *remove_type != RemoveType::Substring {
            process_line(&line.unwrap(), &bloom_filter, min_ngram_size, max_ngram_size,
                         remove_type, filtering_threshold, no_update_bloom_filter, annotate)
        } else {
            process_line_substring(&line.unwrap(), &bloom_filter, max_ngram_size,
                                   no_update_bloom_filter, annotate, substr_seqlen)
        };
        removed_text_bytes += removed_line_bytes;
        total_text_bytes += total_line_bytes;
        if dedup_data.get("text").unwrap().as_str().unwrap().trim().is_empty() {
            fully_skipped += 1
        }
        else {
            serde_json::to_writer(&mut writer, &dedup_data)?;
            writer.write_all(b"\n")?;             
        }        
    }

    // Handle output files
    writer.flush()?;
    let encoder = writer.into_inner().expect("Failed to get encoder");
    encoder.finish().unwrap();
    if fully_skipped < count {
        if is_s3(output_file) {
            let (output_bucket, output_key) = split_s3_path(output_file);
            let client = get_s3_client().await;
            let _ = client
                .put_object()
                .bucket(output_bucket)
                .key(output_key)
                .body(ByteStream::from(output_data))
                .send()
                .await;            
        } else {
            let mut output_file = OpenOptions::new()
                .read(false)
                .write(true)
                .create(true)
                .truncate(true)
                .open(output_file)?;            
            output_file.write_all(&output_data)?;
        }
    }


    match pbar_option {
        Some(pbar) => {
            let pb = pbar.lock().unwrap();
            pb.inc(1);
        }
        None => (),
    }
    Ok((removed_text_bytes, total_text_bytes))
}


fn process_line(line: &String, bloom_filter: &BloomFilter, min_ngram_size: usize, max_ngram_size: usize,
               remove_type: &RemoveType, filtering_threshold: f64, no_update_bloom_filter: bool, annotate: bool) ->  
    (serde_json::Value, usize, usize) {
    // Main BFF logic: processes a single json document
    // Does the following (handling the {paragraph, document, both} cases)
    // 1. Breaks document into units (paragraph/both -> paragraph; document -> full text)
    // 2. For each unit, tokenize and 
    //    a. if num_tokens < min_ngram_size: do nothing, leave this unit intact
    //    b. if num_tokens >= max_ngram_size: break unit into ngram-shingling of max_ngram_size
    //    c. else, full unit is treated as one ngram
    // 3. Check containment of each ngram in bloom filter.
    //    a. If > filtering_threshold contained, mark unit for deletion
    // 4. If unit survives step 3, add all ngrams into bloom filter 
    // 5. BOTH-mode ONLY: If total_contained_ngrams * threshold >= total_ngrams, omit the WHOLE document

    // Outputs are (output_json, total_removed_bytes, total_input_bytes)
    // If annotate is turned on, nothing gets removed, text is left intact, but byte-windows-removed


    let mut data: Value = serde_json::from_str(&line).unwrap();
    let mut total_bytes = 0;
    let mut removed_bytes = 0;
    let text = data["text"].as_str().unwrap();

    // Step 1: Break text into "units"
    let newlines = if *remove_type == RemoveType::Document {
        vec![0, text.len()]
    } else {
        let mut newlines = Vec::new();
        newlines.push(0);
        for i in text.match_indices('\n') {
            newlines.push(i.0);
        }
        newlines.push(text.len());
        newlines
    };
    let mut windows_to_remove = Vec::new();


    let mut total_ngrams = 0;
    let mut total_contained_ngrams = 0;
    for paragraph_window in newlines.windows(2) {
        let paragraph = &text[paragraph_window[0]..paragraph_window[1]];
        total_bytes += paragraph.len();
        

        // Step 2: Tokenize and chunk into ngram shinglings, hash each one for the bff
        let mut hashes: Vec<Vec<u64>> = Vec::new();
        let mut ngram: VecDeque<&str> = VecDeque::with_capacity(max_ngram_size);
        for token in tokenize(paragraph) {
            ngram.push_back(token);
            if ngram.len() >= max_ngram_size { // Step 2b: ngram shingling long enough
                hashes.push(bloom_filter.hashes(&ngram));
                ngram.pop_front();
            }
        }
        // Step 2c: unit is short, but not TOO SHORT
        if hashes.is_empty() && ngram.len() >= min_ngram_size {
            hashes.push(bloom_filter.hashes(&ngram));
        }


        // Step 3: check containment of ngrams
        let contained_ngrams = hashes
            .iter()
            .filter(|ngram| bloom_filter.contains_hashes(ngram))
            .count();
        total_ngrams += hashes.len();
        total_contained_ngrams += contained_ngrams;        
        let number_of_ngrams = hashes.len() as f64;
        //windows_to_remove.ansoteuhoausenh();
        let should_remove = contained_ngrams as f64 / number_of_ngrams > filtering_threshold;
        if  should_remove {
            windows_to_remove.push(paragraph_window);
            removed_bytes += paragraph.len();
        } else if !no_update_bloom_filter { 
            // Step 4: add all ngrams to the bloom filter if we don't remove it
            for ngram in hashes {
                bloom_filter.insert_hashes(&ngram);
            }
        }
    }

    // Step 5: Handle the both case
    let temp_window = vec![0, text.len()];
    if *remove_type == RemoveType::Both && 
        (total_contained_ngrams as f64) / (total_ngrams as f64) > filtering_threshold {
        windows_to_remove.clear();
        windows_to_remove.push(&temp_window);
    }

    // Format outputs: 
    if annotate {
        data["bff_duplicate_spans"] = serde_json::to_value(windows_to_remove).unwrap();
        data["bff_contained_ngram_count"] = serde_json::to_value(total_contained_ngrams).unwrap();
    } else {
        let mut output_paragraphs = String::new();
        let mut last_end = 0;
        for paragraph_window in windows_to_remove {
            output_paragraphs.push_str(&text[last_end..paragraph_window[0]]);
            last_end = paragraph_window[1];
        }
        output_paragraphs.push_str(&text[last_end..]);
        data["text"] = Value::String(output_paragraphs);
    }

    (data, removed_bytes, total_bytes)
}


fn process_line_substring(line: &String, bloom_filter: &BloomFilter, max_ngram_size: usize,
                           no_update_bloom_filter: bool, annotate: bool, substr_seqlen: usize) -> 
    (serde_json::Value, usize, usize) {
    let mut data: Value = serde_json::from_str(&line).unwrap();
    let text = data["text"].as_str().unwrap();
    let mut total_tokens: i32 = 0;

    let total_bytes = text.len();
    //println!("{}", "-".repeat(100));
    //println!("LINE IS {:?}", line);
    //println!("Text length is {}", text.len());
    // Step 1: Get contained ngram indices, and map from ngram/token idx -> text idx
    let mut hashes : Vec<Vec<u64>> = Vec::new(); // Note: hashes[i] is the hash of tokens[i..i + max_ngram_size]
    let mut ngram: VecDeque<&str> = VecDeque::with_capacity(max_ngram_size);
    let mut tokenidx2textidx: Vec<usize> = Vec::new();

    let mut debug_tokens : Vec<&str> = Vec::new();
    for (text_idx, token) in tokenize_indices(text) {
        debug_tokens.push(token);
        total_tokens += 1;
        tokenidx2textidx.push(text_idx);
        ngram.push_back(token);
        if ngram.len() >= max_ngram_size {
            hashes.push(bloom_filter.hashes(&ngram));
            ngram.pop_front();
        }
    }
    if hashes.len() == 0 { // Too short of document, do nothing, return early
        return (data, 0, total_tokens as usize);
    }


    tokenidx2textidx.push(text.len());
    //println!("TOKENS {:?}", debug_tokens);
    //println!("TOKENS2IDX {:?}", tokenidx2textidx);
    let contained_ngram_idxs: Vec<i32> = hashes // idxs are TOKEN/NGRAM indices
        .iter()
        .enumerate()
        .filter(|(_, hash)| bloom_filter.contains_hashes(hash))
        .map(|(i, _)| i as i32)
        .collect();
    let bff_contained_ngram_count = contained_ngram_idxs.len();

    //println!("CONTAINED {:?}", contained_ngram_idxs);
    // Step 2: Collect groups of contained ngrams of length >= substr_seqlen tokens
    // We'll keep track of spans of tokens of length at least substr_seqlen in the to_remove_token_ranges object
    // This has [token_start, token_end] (INCLUSIVE) ranges of long-enough spans of seen tokens
    // AND is bookended by (-1,-1), (#tokens, #tokens)
    let remove_grouplen = (substr_seqlen - max_ngram_size + 1) as i32; 
    let mut to_remove_token_ranges : Vec<(i32, i32)> = Vec::new(); // ranges of TOKEN indices to remove, i.e. [start,end] INCLUSIVE
    let mut cur_group : (i32, i32) = (-1, -1); 
    to_remove_token_ranges.push(cur_group); // bookend LEFT
    for idx in &contained_ngram_idxs {
        let idx = *idx;
        if cur_group == (-1, -1) { // special case for first group
            cur_group = (idx, idx);
        } else if cur_group.1 == idx -1 { // continue growing the current group
            cur_group = (cur_group.0, idx);
        } else { // need to start a new group
            //println!("STARTING NEW GROUP {:?}", cur_group);
            if cur_group.1 - cur_group.0 >= remove_grouplen -1 { // if current group long enough, save it
                to_remove_token_ranges.push((cur_group.0, cur_group.1 + max_ngram_size as i32 -1 ));
            }
            cur_group = (idx, idx); // start a new group
        }
    }
    //println!("OUTPUT CUR GROUP {:?}", cur_group);
    if cur_group.1 - cur_group.0 >= remove_grouplen -1 { // final group (if long enough)
        to_remove_token_ranges.push((cur_group.0, cur_group.1 + max_ngram_size as i32 - 1));
    }
    to_remove_token_ranges.push((total_tokens as i32, total_tokens as i32)); // bookend RIGHT
    //println!("TO REMOVE RANGES {:?}", to_remove_token_ranges);


    // Step 3: 
    // Since we have inclusive ranges of the tokens we remove, we can get ranges of tokens we keep by .windows(2)
    // So we create token-to-keep intervals for which pieces of text to push to output 
    // AND ngram-to-add indices for ngrams that remain here
    let mut output_str = String::new();
    for window in to_remove_token_ranges.windows(2) { // windows indicate the NOT_REMOVED tokens

        let tok_start = window[0].1 + 1;// also the ngram start
        let tok_end = window[1].0;
        let ngram_end = tok_end - max_ngram_size as i32 + 1;
        //println!("ADDING IN TOKENS [{:?})", (tok_start, tok_end));
        //println!("ADDING IN NGRAMS [{:?})", (tok_start, ngram_end));
        //println!("KEEP RANGE {:?} {:?}", tok_start, tok_end);
        if tok_end <= tok_start {
            continue;
        }        

        if !no_update_bloom_filter {
            for ngram_idx in tok_start..ngram_end { // Add ngrams
                bloom_filter.insert_hashes(&hashes[ngram_idx as usize]);
            }
        }
        // And then push the text to output
        output_str.push_str(&text[tokenidx2textidx[tok_start as usize]..tokenidx2textidx[tok_end as usize]]);
    }
    //println!("OUTPUT STR {:?}", output_str);
    if annotate {
        // Spans here are like [lo,hi) ... i.e. semi-open intervals
        let mut duplicate_char_spans : Vec<(usize, usize)> = Vec::new();
        for i in 1..to_remove_token_ranges.len() - 1 {
            let (s, e) = to_remove_token_ranges[i];
            duplicate_char_spans.push((tokenidx2textidx[s as usize], tokenidx2textidx[e as usize + 1]));
        }

        data["bff_duplicate_spans"] = serde_json::to_value(duplicate_char_spans).unwrap();
        data["bff_contained_ngram_count"] = serde_json::to_value(bff_contained_ngram_count).unwrap();
    } else {
        data["text"] = Value::String(output_str.trim_end().to_string());
    }

    (data, total_bytes - output_str.len() as usize, total_bytes as usize)
}


fn process_line_exact(line: &String, bloom_filter: &BloomFilter, no_update_bloom_filter: bool, annotate: bool) ->
    (serde_json::Value, usize, usize) {
    // Hacky "exact dedup" using bloom filters
    // Just hashes the WHOLE text 

    let mut data: Value = serde_json::from_str(&line).unwrap();
    let mut removed_bytes = 0;

    let text = data["text"].as_str().unwrap();
    let total_bytes = text.len();
    let mut fake_ngram: VecDeque<&str> = VecDeque::with_capacity(1);
    fake_ngram.push_back(text);
    let hashes = bloom_filter.hashes(&fake_ngram);

    if bloom_filter.contains_hashes(&hashes) {
        // If we've seen this before, modify text XOR annotate 
        if annotate {
            data["bff_exact_duplicate"] = serde_json::to_value(true).unwrap();
        } else {
            data["text"] = Value::String(String::new());
        }
        removed_bytes = total_bytes;
    } else {
        // If we haven't seen this before, insert it
        if !no_update_bloom_filter {
            bloom_filter.insert_hashes(&hashes);
        }
    }   

    return (data, removed_bytes, total_bytes)
}




/*========================================================
=                       I/O Stuff                        =
========================================================*/

async fn expand_dirs(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {    
    // Can handle both local and s3 files/directories
    // Note that this function is async because we need to wait for all the s3 files to get expanded
    // Also note that the output vector is SORTED (in case S3 has inconsistent list_objects_v2 ordering)
    let mut files: Vec<PathBuf> = Vec::new();
    for path in paths {
        if is_s3(path) {
            let s3_result = expand_s3_dirs(path).await?;
            for file in s3_result {
                files.push(file.clone());
            }
        } else if path.is_dir() {
            let path_str = path
                .to_str()
                .ok_or_else(|| anyhow!("invalid path '{}'", path.to_string_lossy()))?;
            for entry in glob(&format!("{}/**/*.json*.gz", path_str))? {
                files.push(entry?.to_path_buf());
            }
        } else {
            files.push(path.clone());
        }
    }   
    files.sort();
    Ok(files)
}



async fn expand_s3_dirs(s3_uri: &PathBuf) -> Result<Vec<PathBuf>> {
    let mut s3_files: Vec<PathBuf> = Vec::new();

    let (bucket, prefix) = split_s3_path(s3_uri);
    let region_provider = RegionProviderChain::default_provider();
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    let client = Client::new(&config);

    let mut response = client
        .list_objects_v2()    
        .bucket(bucket.to_owned())
        .prefix(prefix.to_owned())
        .into_paginator()
        .send();

    while let Some(result) = response.next().await {
        match result {
            Ok(output) => {
                for object in output.contents() {
                    let key = object.key().unwrap();
                    if !(key.ends_with(".jsonl.gz") || key.ends_with(".json.gz") || key.ends_with(".jsonl.zstd")) {
                        continue;
                    }
                    let mut s3_file = PathBuf::from("s3://");
                    s3_file.push(bucket.clone());
                    s3_file.push(key);
                    s3_files.push(s3_file);
                }
            }
            Err(err) => {
                eprintln!("Error collecting S3 files | {err:?}")
            }
        }
    }
    Ok(s3_files)
}


async fn get_object_with_retry(bucket: &str, key: &str, num_retries: usize) -> Result<GetObjectOutput, aws_sdk_s3::Error> {
    let mut attempts = 0;
    let base_delay = Duration::from_millis(100);
    let max_delay = Duration::from_millis(2000);

    let mut rng = rand::thread_rng();
    let client = get_s3_client().await;
    loop {
        match client.get_object().bucket(bucket).key(key).send().await {
            Ok(response) => return Ok(response),
            Err(e) if attempts < num_retries => {
                // Calculate delay for exponential backoff, add some randomness so multiple threads don't access at the
                // same time.
                println!("Error {}/{}: {}", e, attempts, num_retries);
                let random_delay =  rng.gen_range(Duration::from_millis(0)..Duration::from_millis(1000));
                let mut exponential_delay = base_delay * 2u32.pow(attempts as u32);
                if exponential_delay > max_delay {
                    exponential_delay = max_delay;
                }
                sleep(exponential_delay + random_delay).await;
                attempts += 1;
            }
            Err(e) => {
                println!("Too many errors reading: {}. Giving up.", key);
                return Err(e.into());
            }
        }
    }
}



async fn get_reader_from_s3(path: &PathBuf, num_retries: Option<usize>) -> Result<BufReader<Cursor<Vec<u8>>>>{
    // Gets all the data from an S3 file and loads it into memory and returns a Bufreader over it
    let num_retries = num_retries.unwrap_or(5);
    let (s3_bucket, s3_key) = split_s3_path(path);
    let object = get_object_with_retry(&s3_bucket, &s3_key, num_retries).await?;
    let body_stream = object.body.into_async_read();
    let mut data = Vec::new();

    if path.extension().unwrap() == "zstd" {
        let zstd = asyncZstd::new(body_stream);
        let mut reader = tBufReader::with_capacity(1024 * 1024, zstd);
        reader.read_to_end(&mut data).await.expect("Failed to read data {:path}");
    } else {
        let gz = asyncGZ::new(body_stream);
        let mut reader = tBufReader::with_capacity(1024 * 1024, gz);
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await.expect("Failed to read data {:path}");
    }
    let cursor = Cursor::new(data);
    Ok(BufReader::new(cursor))
}

fn create_dir_if_not_exists(path: &PathBuf) -> Result<(), std::io::Error> {
    if is_s3(path) {
        return Ok(()); // S3 bucket/prefixes always already exist
    }
    match create_dir_all(path) {
        Ok(_) => Ok(()),
        Err(err) => {
            if err.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(err)
            }
        }
    }
}

fn split_s3_path(path: &PathBuf) -> (String, String) {
    // Splits s3_uri into (bucket, key)
    let path_str = path.to_str().expect("Invalid path");

    let path_without_scheme = path_str
        .strip_prefix("s3://")
        .expect("Path must start with 's3://'");

    let slash_index = path_without_scheme
        .find('/')
        .expect("Path must contain a slash after the bucket name");

    let bucket = &path_without_scheme[..slash_index];
    let key = &path_without_scheme[slash_index + 1..];
    (bucket.to_string(), key.to_string())
}


fn is_s3(path: &PathBuf) -> bool {
    if let Some(s) = path.to_str() {
        s.starts_with("s3://")
    } else {
        false
    }
}

async fn get_s3_client() -> Client {
    let region_provider = RegionProviderChain::default_provider();
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    Client::new(&config)
}


fn get_output_filename(inputs: &[PathBuf], input_filename: &PathBuf, output_directory: &PathBuf) -> PathBuf {
    // More fancy output-file naming that no longer assumes unique inputs
    let matching_prefix = inputs
        .iter()
        .find(|pfx| input_filename.starts_with(pfx))
        .expect("No matching prefix found?!?");

    let relative_path = input_filename.strip_prefix(matching_prefix).unwrap();
    output_directory.clone().join(relative_path)

}


/*=============================================================
=                       Main Function                         =
=============================================================*/


#[tokio::main]
async fn main() -> Result<()> {
    let args = ArgParser::parse();

    match &args.command {
        Commands::Bff {inputs, output_directory, bloom_filter_file, expected_ngram_count,
                       fp_rate, min_ngram_size, max_ngram_size, substr_seqlen, filtering_threshold, 
                       remove_type, num_hashers, no_update_bloom_filter, annotate,
                       threads, no_save_bloom_filter, no_progress_bar, shard_num, total_shards} =>
        {
            assert!(shard_num < total_shards, "Shard num must be < total shards");
            bff(inputs, output_directory, bloom_filter_file, expected_ngram_count,
                fp_rate, min_ngram_size, max_ngram_size, substr_seqlen, filtering_threshold,
                remove_type, num_hashers, no_update_bloom_filter, annotate,
                threads, no_save_bloom_filter, no_progress_bar, shard_num, total_shards).await?;
        },
        Commands::Sysreq {expected_ngram_count, num_hashers, fp_rate} => {
            let bff_size = compute_bloom_size(*fp_rate, *expected_ngram_count, false, *num_hashers);
            let num_hashers = if *num_hashers == 0 {
                    BloomFilter::optimal_number_of_hashers(bff_size, *expected_ngram_count)
                } else {*num_hashers};
            println!("To handle {} tokens with fp rate {}, you'd need a filter of size {} and {} hashers",
                     expected_ngram_count, fp_rate, human_bytes(bff_size as f64), num_hashers);            
        },

    }
    Ok(())
}



async fn bff(inputs: &Vec<PathBuf>, output_directory: &PathBuf, bloom_filter_file: &Option<PathBuf>,
       expected_ngram_count: &usize, fp_rate: &f64, min_ngram_size: &usize, max_ngram_size: &usize,
       substr_seqlen: &usize, filtering_threshold: &f64, remove_type: &RemoveType, num_hashers: &usize, 
       no_update_bloom_filter: &bool, annotate: &bool, threads: &usize, no_save_bloom_filter: &bool,
       no_progress_bar: &bool, shard_num: &usize, total_shards: &usize) -> Result<()> {


    // SETUP PHASE:
    // Set up {output_location, filter, inputs, threading, progress bar}
    let start_time = Instant::now();
    create_dir_if_not_exists(output_directory).unwrap(); 
    let bloom_filter = Arc::new(BloomFilter::from_args(bloom_filter_file.clone(), *expected_ngram_count, 
                                *fp_rate, *num_hashers)); 


    // Setup input files
    let all_inputs = expand_dirs(inputs).await?;

    let mut shard: Vec<PathBuf> = Vec::new();
    let mut idx = *shard_num;
    while idx < all_inputs.len() {
        shard.push(all_inputs[idx].clone());
        idx += total_shards;
    }
    let mut rng = thread_rng();
    shard.shuffle(&mut rng); 
    shard.truncate(100); // REMOVE! 
    // Setup threads
    let threads = if *threads == 0 {
        available_parallelism().unwrap().get()
    } else {
        *threads
    };
    // Setup progress bar
    let pbar = ProgressBar::new(shard.len() as u64)
        .with_style(
            ProgressStyle::with_template(
                "Files {human_pos}/{human_len} [{elapsed_precise}/{duration_precise}] [{wide_bar:.cyan/blue}]",
            ).unwrap()
        );
    let pbar = Arc::new(Mutex::new(pbar));
    if !no_progress_bar {
        pbar.lock().unwrap().inc(0); // Makes pbar show up with 0/N files complete
    }    
    println!("Completed setup phase in {:?} seconds", start_time.elapsed().as_secs());


    // LOOP PHASE(using threadpool)
    let loop_start_time = Instant::now();
    let total_bytes = Arc::new(Mutex::new(0));
    let removed_bytes = Arc::new(Mutex::new(0));
    let threadpool = ThreadPool::new(threads);
    for input in shard {
        //let output = output_directory.clone().join(input.file_name().unwrap());
        let output = get_output_filename(inputs, &input, output_directory);
        println!("INPUT OUTPUT {:?} | {:?}", input, output);
        let bloom_filter = bloom_filter.clone();
        let pbar_option: Option<Arc<Mutex<ProgressBar>>> = if *no_progress_bar {
            None
        } else {
            Some(pbar.clone())
        };
        let min_ngram_size = min_ngram_size.clone();
        let max_ngram_size = max_ngram_size.clone();
        let substr_seqlen = substr_seqlen.clone();
        let filtering_threshold = filtering_threshold.clone();
        let remove_type = remove_type.clone();
        let no_update_bloom_filter = no_update_bloom_filter.clone();
        let annotate = annotate.clone();
        let no_progress_bar = no_progress_bar.clone();
        let total_bytes = Arc::clone(&total_bytes);
        let removed_bytes = Arc::clone(&removed_bytes);


        threadpool.execute(move || {
            if no_progress_bar {
                println!("Processing {input:?}...");
            }

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();            
            let result = rt.block_on(
                process_file(
                    &input,
                    &output,
                    &bloom_filter,
                    max_ngram_size,
                    min_ngram_size,
                    substr_seqlen, 
                    &remove_type,
                    filtering_threshold.clone(),
                    no_update_bloom_filter.clone(),
                    annotate.clone(),
                    &pbar_option
                    )                
                );
            match result {
                Ok(outputs) => {
                    let (removed_doc_bytes, total_doc_bytes) = outputs;
                    let mut total_guard = total_bytes.lock().unwrap();
                    *total_guard += total_doc_bytes;
                    let mut removed_guard = removed_bytes.lock().unwrap();
                    *removed_guard += removed_doc_bytes;                        
                },
                Err(err) => {
                    eprintln!("Error processing {:?}; {:?}", input, err);
                }
            }        
        });
    }
    threadpool.join();
    println!("Completed filtering all files in {:?} seconds", 
             loop_start_time.elapsed().as_secs());    


    // FINALIZE PHASE 
    // Save bloom filter
    match &bloom_filter_file {
        Some(path) => {
            if !no_update_bloom_filter && !no_save_bloom_filter {
                let write_start_time = Instant::now();
                println!("Writing bloom filter to {:?}...", path);
                bloom_filter.write_to_file(&path).unwrap();
                println!("...Bloom filter written in {:?} seconds.", write_start_time.elapsed().as_secs());                
            }

        }
        _ => {}
    }
    // Print out summary
    println!("After running, BFF sparsity was {:?}", bloom_filter.calculate_sparsity());
    println!("Completed full BFF run in {:?} seconds", start_time.elapsed().as_secs());

    let total_bytes = *total_bytes.lock().unwrap();
    let removed_bytes = *removed_bytes.lock().unwrap();
    println!("Stats: Saw {} of text | Removed {} of them",
             human_bytes(total_bytes as f64), removed_bytes as f64 / total_bytes as f64);   
    Ok(())
}