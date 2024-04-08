BFF
===

The big friendly filter 😁
(originally written by Dirk @ AI2, updated by me)

Getting started
---------------

1. Install Rust on your machine.
    1. `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
    2. Add `~/.cargo/bin` to your `PATH` environment variable.
2. Run `cargo build --release`. It places the binary at `target/release/bff`.
3. Run `./target/release/bff --help` to see the available options.


Examples
--------
There are three modes `bff` (local input -> local output), `bff-remote` (S3 input -> S3 output), and `sysreq` (for assessing system requirements). We always need an input, output, false positive rate, and expected number of ngrams. But  then there's some optional hyperparameters:

- `--min-ngram-size`: In pargraph/both mode, we ignore any paragraphs shorter than this. Defaults to 5.
- `--max-ngram-size`: The "working width" of shinglings of ngrams: e.g., for long paragraphs/documents, we check membership over ngrams of this size. Defaults to 13.
- `--filtering-threshold`: If at least this fraction of ngrams is present, we remove the entire paragraph/document. Defaults to 0.8

And some REMOTE ONLY arguments:
- `--shard-num`: For large nummbers of files, sharding is helpful. This selects some subset of the files. Defaults to 0
- `--num-shards`: Dictates how many shards we have. Defaults to 1.

### Deduplicating local files:
For files that exist locally, say a directory `to_be_deduped/`, we can output deduplicated versions of these files in `has_been_deduped/` like:
```cargo run --release bff \
   --inputs to_be_deduped \
   --output-directory has_been_deduped \
   --expected-ngram-count 12345678 \
   --fp-rate 0.01
```

### Deduplicating remote files
For files that exist on S3, say with the prefix `s3://my-bucket/to_be_deduped/`, we can output deduplicated versions of these files in `s3://my-bucket/has_been_deduped` like:
``` cargo run --release bff-remote \
--bucket my-bucket \
--input-dir to_be_deduped \
--output_dir has_been_deduped \
--expected-ngram-count 12345678 \\
--fp-rate 0.01
```

There's also some options to preload or save the bloom filter itself, but you can check the code for those.

